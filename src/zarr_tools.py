#!/usr/bin/env python3
"""
Zarr-specific utility functions for optimized data storage.

Zarr 3 SAFE VERSION:
- Uses only zarr.codecs (no numcodecs)
- No encoding chunk overrides (handled by xarray/dask)
- Minimal metadata to avoid Zarr v3 codec conflicts
"""

import xarray as xr
import zarr
import shutil
import time
import threading
import logging
from pathlib import Path
from dask.diagnostics import ProgressBar, ResourceProfiler, CacheProfiler
from . import chunk_tools

# Configure logging
logger = logging.getLogger(__name__)


# ============================================================
# Progress monitor (unchanged, safe)
# ============================================================

def monitor_zarr_write_progress(output_path, expected_chunks, check_interval=30):

    stop_event = threading.Event()

    def progress_monitor():
        start_time = time.time()
        last_file_count = 0
        last_update_time = start_time

        while not stop_event.is_set():
            try:
                current_time = time.time()
                elapsed = current_time - start_time

                precip_dir = output_path / "precipitation"

                if precip_dir.exists():
                    file_count = len([
                        f for f in precip_dir.iterdir()
                        if f.is_file() and not f.name.startswith(".")
                    ])

                    if file_count > last_file_count or (current_time - last_update_time) >= check_interval:

                        time_chunks, spatial_chunks = expected_chunks
                        total_expected = time_chunks * spatial_chunks

                        if total_expected > 0:
                            progress_pct = (file_count / total_expected) * 100
                            eta_min = (
                                (elapsed / max(file_count, 1))
                                * (total_expected - file_count)
                            ) / 60 if file_count > 0 else 0

                            logger.info(
                                f"📊 Zarr write: {file_count}/{total_expected} chunks "
                                f"({progress_pct:.1f}%) | "
                                f"{elapsed/60:.1f}min elapsed | "
                                f"ETA: {eta_min:.1f}min"
                            )
                        else:
                            logger.info(
                                f"📊 Zarr write: {file_count} chunks | "
                                f"{elapsed/60:.1f}min elapsed"
                            )

                        last_file_count = file_count
                        last_update_time = current_time

                    elif file_count == last_file_count and (current_time - last_update_time) >= 60:
                        logger.info(
                            f"💓 Zarr heartbeat: {file_count} chunks | "
                            f"{elapsed/60:.1f}min elapsed"
                        )
                        last_update_time = current_time

                stop_event.wait(check_interval)

            except Exception as e:
                logger.debug(f"Progress monitor error: {e}")
                stop_event.wait(check_interval)

    thread = threading.Thread(target=progress_monitor, daemon=True)
    thread.start()

    return thread, stop_event


# ============================================================
# Zarr writer (Zarr 3 SAFE)
# ============================================================



def write_zarr_with_monitoring(
    ds_remap,
    output_zarr,
    time_chunk_size=12,
    zoom=9,
    overwrite=False,
):

    # --------------------------------------------------------
    # rechunk
    # --------------------------------------------------------

    spatial_chunk_size = chunk_tools.compute_chunksize(
        order=zoom
    )

    logger.info(
        f"Rechunking: "
        f"time={time_chunk_size}, "
        f"cell={spatial_chunk_size}"
    )

    ds_remap_chunked = ds_remap.chunk(
        {
            "time": time_chunk_size,
            "cell": spatial_chunk_size,
        }
    )

    # --------------------------------------------------------
    # VERY IMPORTANT:
    # remove inherited encoding metadata
    # from source CCIC zarr
    # --------------------------------------------------------

    logger.info(
        "Removing inherited variable encodings"
    )

    for var in ds_remap_chunked.variables:

        ds_remap_chunked[var].encoding = {}

    # --------------------------------------------------------
    # diagnostics
    # --------------------------------------------------------

    logger.info("Dataset variables:")

    for var in ds_remap_chunked.variables:

        logger.debug(
            f"{var}: "
            f"{ds_remap_chunked[var].encoding}"
        )

    # --------------------------------------------------------
    # output path handling
    # --------------------------------------------------------

    output_path = Path(output_zarr)

    if output_path.exists():

        if overwrite:

            logger.info(
                f"Overwriting: {output_path}"
            )

            shutil.rmtree(output_path)

        else:

            raise FileExistsError(
                f"{output_path} already exists"
            )

    # --------------------------------------------------------
    # Zarr v3 codec
    # --------------------------------------------------------

    from zarr.codecs import (
        BloscCodec,
        BloscShuffle,
    )

    compressor = BloscCodec(
        cname="zstd",
        clevel=3,
        shuffle=BloscShuffle.bitshuffle,
    )

    # --------------------------------------------------------
    # encoding
    # --------------------------------------------------------

    encoding = {
        var_name: {
            "compressor": compressor,
        }
        for var_name in ds_remap_chunked.data_vars
    }

    logger.info(
        "Using Zarr v3 BloscCodec compression"
    )

    # --------------------------------------------------------
    # logging
    # --------------------------------------------------------

    logger.info("🔄 Starting Zarr write...")

    start = time.time()

    logger.info(
        f"Dataset size: "
        f"{ds_remap_chunked.nbytes / 1024**3:.2f} GB"
    )

    # --------------------------------------------------------
    # chunk diagnostics
    # --------------------------------------------------------

    try:

        time_chunks = len(
            ds_remap_chunked.chunks["time"]
        )

        spatial_chunks = len(
            ds_remap_chunked.chunks["cell"]
        )

    except Exception:

        time_chunks = 0
        spatial_chunks = 0

    logger.info(
        f"Chunks: "
        f"{time_chunks} × {spatial_chunks}"
    )

    # --------------------------------------------------------
    # progress monitor
    # --------------------------------------------------------

    monitor, stop_event = (
        monitor_zarr_write_progress(
            output_path,
            (time_chunks, spatial_chunks),
        )
    )

    try:

        # ----------------------------------------------------
        # diagnostics
        # ----------------------------------------------------

        logger.info("Encoding summary:")

        for name, enc in encoding.items():

            logger.info(
                f"{name}: {enc}"
            )

        # ----------------------------------------------------
        # write
        # ----------------------------------------------------


        print(f"Dataset size: {ds_remap_chunked.nbytes / 1024**3:.2f} GB")

        for var in ds_remap_chunked.data_vars:
            arr = ds_remap_chunked[var]
            print(f"{var}: shape={arr.shape}, "
                  f"dtype={arr.dtype}, "
                  f"chunks={arr.chunks}")

        import psutil, os
        logger.info(f"RAM used: {psutil.Process(os.getpid()).memory_info().rss/1e9:.2f} GB")
        
        write_task = ds_remap_chunked.to_zarr(
            output_path,
            encoding=encoding,
            zarr_format=3,
            consolidated=False,
            compute=True,)

        logger.info(
            "Executing Zarr write..."
        )

        #with (
         #   ProgressBar(),
         #   ResourceProfiler(),
         #   CacheProfiler(),
        #):

            #write_task.compute()

    except Exception as e:

        logger.exception(
            "Zarr write failed"
        )

        raise

    finally:

        stop_event.set()

        monitor.join(timeout=2)

    elapsed = time.time() - start

    logger.info(
        "✅ Zarr write completed"
    )

    logger.info(
        f"Time: {elapsed/60:.1f} min"
    )

    return ds_remap_chunked, elapsed
