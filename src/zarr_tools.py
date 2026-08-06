#!/usr/bin/env python3
"""
Zarr-specific utility functions for optimized data storage.

This module contains functions for:
- Zarr write progress monitoring
- Optimized Zarr writing with chunking and compression
- Zarr encoding configuration
"""

import xarray as xr
import numpy as np
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


def monitor_zarr_write_progress(output_path, expected_chunks, check_interval=30):
    """
    Monitor Zarr write progress by checking file creation in the background.
    
    Parameters:
    -----------
    output_path : Path
        Path to the Zarr directory being written
    expected_chunks : tuple
        Expected number of chunks (time_chunks, spatial_chunks)
    check_interval : int
        Check interval in seconds
        
    Returns:
    --------
    tuple : (monitor_thread, stop_event)
        Thread object and Event to signal stopping
    """
    
    stop_event = threading.Event()
    
    def progress_monitor():
        start_time = time.time()
        last_file_count = 0
        last_update_time = start_time
        
        while not stop_event.is_set():
            try:
                current_time = time.time()
                elapsed = current_time - start_time
                
                # Count files in the precipitation directory
                precip_dir = output_path / "precipitation"
                if precip_dir.exists():
                    file_count = len([f for f in precip_dir.iterdir() if f.is_file() and not f.name.startswith('.')])
                    
                    # Show progress if files increased OR if it's been a while since last update
                    if file_count > last_file_count or (current_time - last_update_time) >= check_interval:
                        time_chunks, spatial_chunks = expected_chunks
                        total_expected = time_chunks * spatial_chunks
                        
                        if total_expected > 0:
                            progress_pct = (file_count / total_expected) * 100
                            eta_min = ((elapsed / max(file_count, 1)) * (total_expected - file_count)) / 60 if file_count > 0 else 0
                            logger.info(f"📊 Zarr write: {file_count}/{total_expected} chunks ({progress_pct:.1f}%) | {elapsed/60:.1f}min elapsed | ETA: {eta_min:.1f}min")
                        else:
                            logger.info(f"📊 Zarr write: {file_count} chunks written | {elapsed/60:.1f}min elapsed | Processing...")
                        
                        last_file_count = file_count
                        last_update_time = current_time
                    
                    # If no progress for a long time, show heartbeat
                    elif file_count == last_file_count and (current_time - last_update_time) >= 60:
                        logger.info(f"💓 Zarr write heartbeat: {file_count} chunks | {elapsed/60:.1f}min elapsed | Still processing...")
                        last_update_time = current_time
                
                # Sleep with check for stop event
                stop_event.wait(check_interval)
                
            except Exception as e:
                logger.debug(f"Progress monitor error: {e}")
                if not stop_event.is_set():
                    stop_event.wait(check_interval)
    
    # Start monitoring thread
    monitor_thread = threading.Thread(target=progress_monitor, daemon=True)
    monitor_thread.start()
    return monitor_thread, stop_event


def write_zarr_with_monitoring(ds_remap, output_zarr, time_chunk_size=48, zoom=9, overwrite=False):
    """
    Write remapped dataset to Zarr with optimal chunking, progress monitoring, and profiling.
    
    Parameters:
    -----------
    ds_remap : xr.Dataset
        Remapped dataset to write
    output_zarr : str
        Output Zarr path
    time_chunk_size : int
        Time chunk size for Zarr writing (default: 48)
    zoom : int
        HEALPix zoom level for optimal spatial chunking
    overwrite : bool
        If True, overwrite existing Zarr files
        
    Returns:
    --------
    tuple : (ds_remap_chunked, write_time_seconds)
        Chunked dataset and write time in seconds
    """
    # Calculate optimal spatial chunk size based on zoom level
    spatial_chunk_size = chunk_tools.compute_chunksize(order=zoom)
    logger.info(f"Using spatial chunk size: {spatial_chunk_size} (zoom {zoom})")
    
    # Rechunk for optimal Zarr writing with proper chunk sizes
    logger.info(f"Rechunking for Zarr writing: time={time_chunk_size}, spatial={spatial_chunk_size}")
    ds_remap_chunked = ds_remap.chunk({
        'time': time_chunk_size,
        'cell': spatial_chunk_size  # Use optimal spatial chunking
    })
    
    # Use direct xarray to_zarr for better parallel performance
    logger.info(f"Writing to Zarr with parallel processing: {output_zarr}")
    output_path = Path(output_zarr)
    
    # Check for existing Zarr file and handle overwrite option
    if output_path.exists():
        if overwrite:
            logger.info(f"Overwriting existing Zarr file: {output_path}")
            shutil.rmtree(output_path)
        else:
            raise FileExistsError(
                f"Zarr file already exists: {output_path}\n"
                f"Use overwrite=True to replace it, or choose a different output path."
            )
    
    # Create encoding for efficient storage using dynamic approach
    encoding = {}
    
    # Apply encoding to all data variables in the dataset
    for var_name in ds_remap_chunked.data_vars:
        var = ds_remap_chunked[var_name]
        
        # Check if the variable has floating point data
        if var.dtype.kind == 'f':  # floating point
            encoding[var_name] = {
                'compressor': zarr.Blosc(cname='zstd', clevel=3, shuffle=2),
                'chunks': (time_chunk_size, spatial_chunk_size),  # Time and spatial chunking
                'dtype': 'float32'
            }
        else:
            # For non-floating point data, preserve the original dtype
            encoding[var_name] = {
                'compressor': zarr.Blosc(cname='zstd', clevel=3, shuffle=2),
                'chunks': (time_chunk_size, spatial_chunk_size),
                'dtype': var.dtype
            }
    
    # Set encoding for coordinate variables based on their actual data type
    # Note: Do NOT specify dtype for 'time' coordinate - let xarray/zarr handle it automatically
    # Specifying 'time': {'dtype': 'datetime64[ns]'} causes time coordinate corruption
    if 'cell' in ds_remap_chunked.dims and 'cell' in ds_remap_chunked.coords:
        cell_var = ds_remap_chunked.coords['cell']
        if cell_var.dtype.kind == 'i':  # integer
            encoding['cell'] = {'dtype': 'int32'}
        elif cell_var.dtype.kind == 'f':  # floating point
            encoding['cell'] = {'dtype': 'float32'}

    # Write to Zarr with progress monitoring
    logger.info("🔄 Starting Zarr write operation...")
    zarr_start_time = time.time()
    logger.info(f"Dataset size: {ds_remap_chunked.nbytes / 1024**3:.2f} GB")
    
    # Calculate expected number of chunks for progress monitoring
    time_chunks = len(ds_remap_chunked.chunks['time'])
    spatial_chunks = len(ds_remap_chunked.chunks['cell'])
    total_chunks = time_chunks * spatial_chunks
    logger.info(f"Expected chunks: {time_chunks} time × {spatial_chunks} spatial = {total_chunks} total")
    
    # Show chunk size information for optimization
    chunk_size_mb = (ds_remap_chunked.chunks['time'][0] * ds_remap_chunked.chunks['cell'][0] * 4) / 1024**2  # 4 bytes for float32
    logger.info(f"Individual chunk size: ~{chunk_size_mb:.1f} MB")
    logger.info(f"Total write workload: ~{total_chunks * chunk_size_mb / 1024:.1f} GB")
    
    # Start progress monitoring in background
    progress_monitor, stop_event = monitor_zarr_write_progress(output_path, (time_chunks, spatial_chunks))
    
    try:
        # Use delayed computation with progress bar and profiling
        logger.info("Computing Zarr write tasks...")
        write_task = ds_remap_chunked.to_zarr(
            output_path,
            encoding=encoding,
            compute=False,  # Create delayed computation
            consolidated=True
        )
        
        logger.info("Executing Zarr write with progress monitoring...")
        logger.info("📈 Progress updates every 30 seconds - this may take a while for large datasets")
        
        # Use profilers to monitor resource usage
        with ProgressBar(), ResourceProfiler() as rprof, CacheProfiler() as cprof:
            write_task.compute()
            
        # Log resource usage summary (if available)
        try:
            if hasattr(rprof, 'results') and len(rprof.results) > 0:
                # ResourceProfiler results are ResourceData objects with different attributes
                result = rprof.results[0]
                if hasattr(result, 'cpu'):
                    logger.info(f"Resource usage - CPU: {result.cpu:.1f}%, Memory: {getattr(result, 'memory', 'N/A')}")
                else:
                    logger.info(f"Resource profiling completed (detailed metrics not available)")
            else:
                logger.info("Resource profiling completed")
        except Exception as profile_error:
            logger.debug(f"Resource profiling error: {profile_error}")
            logger.info("Resource profiling completed (metrics unavailable)")
            
    except Exception as e:
        logger.error(f"Error during Zarr write: {e}")
        raise
    finally:
        # Stop progress monitor gracefully
        stop_event.set()
        progress_monitor.join(timeout=2)  # Wait up to 2 seconds for thread to finish
    
    zarr_time = time.time() - zarr_start_time
    logger.info("✅ Zarr write completed successfully!")
    logger.info(f"Zarr write completed in {zarr_time/60:.1f} minutes")

    return ds_remap_chunked, zarr_time


def write_zarr_region(ds_remap, store_path, time_chunk_size=24, zoom=9):
    """
    Write a dataset into a pre-existing time region of a shared Zarr store.

    Companion to write_zarr_with_monitoring() for parallel multi-task writes:
    many independent processes (e.g. one per year in a SLURM job array) each
    write a disjoint time slice into ONE store created ahead of time by
    scripts/init_healpix_store.py, instead of each writing a separate store
    that must later be merged.

    Safety depends entirely on time-chunk alignment: the region this dataset
    occupies in the store MUST start and end on a multiple of the store's
    time chunk size, so that no two concurrent writers ever touch the same
    chunk file. This is verified (along with the exact time values and
    variable set) *before* any bytes are written, and xarray's own
    `safe_chunks` check (left at its default of True) verifies it again.

    Parameters:
    -----------
    ds_remap : xr.Dataset
        Remapped dataset to write. Must have a 'time' dimension whose values
        are an exact, contiguous subset of the store's existing time axis.
    store_path : str
        Path to the PRE-EXISTING Zarr store (see scripts/init_healpix_store.py).
        Unlike write_zarr_with_monitoring(), this function never creates or
        deletes a store - it only writes into one that already exists.
    time_chunk_size : int
        Must match the time chunk size the store was created with (default: 24)
    zoom : int
        HEALPix zoom level, used to compute the spatial chunk size so the
        written blocks line up with the store's on-disk chunks

    Returns:
    --------
    tuple : (ds_remap_chunked, write_time_seconds)
        Chunked dataset and write time in seconds

    Raises:
    -------
    FileNotFoundError : If store_path does not exist
    ValueError : If the dataset's time range isn't found at a chunk-aligned
                 offset in the store, or if its variables aren't a subset of
                 the store's variables

    Notes:
    ------
    - Does NOT write consolidated metadata (consolidated=False) - with many
      concurrent writers, that must be done exactly once after all tasks
      finish, e.g. `zarr.consolidate_metadata(store_path)`.
    - Coordinates that lack a 'time' dimension ('cell', 'crs' for HEALPix
      output) are dropped before writing - they were already written once
      when the store was initialized, and a region write only accepts
      variables along the dimension(s) being regioned.
    """
    store_path = Path(store_path)
    if not store_path.exists():
        raise FileNotFoundError(
            f"Region store does not exist: {store_path}\n"
            f"Create it first with scripts/init_healpix_store.py"
        )

    logger.info(f"Opening region store for alignment check: {store_path}")
    store_ds = xr.open_zarr(store_path, consolidated=True)
    store_time = store_ds['time'].values

    n_time = ds_remap.sizes['time']
    ds_start = ds_remap['time'].values[0]
    ds_end = ds_remap['time'].values[-1]

    i0 = int(np.searchsorted(store_time, ds_start))
    i1 = i0 + n_time

    # Verify exact endpoint match - not just "close enough" - before touching
    # anything. searchsorted finds an insertion point even for values that
    # aren't present, so this equality check is what actually confirms
    # ds_remap is a real, contiguous subset of the store's time axis.
    if i0 >= len(store_time) or store_time[i0] != ds_start:
        raise ValueError(
            f"Dataset start time {ds_start} not found in region store's time axis "
            f"(store spans {store_time[0]} to {store_time[-1]})"
        )
    if i1 > len(store_time) or store_time[i1 - 1] != ds_end:
        found = store_time[i1 - 1] if i1 <= len(store_time) else 'out of range'
        raise ValueError(
            f"Dataset end time {ds_end} not found at the expected offset in region store "
            f"(expected store_time[{i1 - 1}] == {ds_end}, got {found})"
        )

    # The load-bearing safety check: without chunk alignment, two concurrent
    # writers could race on the same chunk file.
    if i0 % time_chunk_size != 0 or i1 % time_chunk_size != 0:
        raise ValueError(
            f"Region [{i0}:{i1}] is not aligned to the store's time_chunk_size="
            f"{time_chunk_size}. Writing an unaligned region risks corrupting a "
            f"neighboring writer's chunk - refusing.\n"
            f"i0 % {time_chunk_size} = {i0 % time_chunk_size}, "
            f"i1 % {time_chunk_size} = {i1 % time_chunk_size}"
        )

    missing_vars = set(ds_remap.data_vars) - set(store_ds.data_vars)
    if missing_vars:
        raise ValueError(
            f"Dataset has variable(s) not present in region store: {missing_vars}\n"
            f"Store variables: {list(store_ds.data_vars)}"
        )
    store_ds.close()

    logger.info(f"Region-write target: time[{i0}:{i1}] ({ds_start} to {ds_end}), "
                f"{n_time} steps")

    # Calculate optimal spatial chunk size based on zoom level (must match
    # the chunking used when the store was initialized)
    spatial_chunk_size = chunk_tools.compute_chunksize(order=zoom)
    logger.info(f"Rechunking for region write: time={time_chunk_size}, spatial={spatial_chunk_size}")
    ds_remap_chunked = ds_remap.chunk({
        'time': time_chunk_size,
        'cell': spatial_chunk_size
    })

    # Drop coords that don't vary along 'time' - a region write only accepts
    # variables along the dimension(s) being regioned, and 'cell'/'crs' were
    # already written once when the store was initialized.
    drop_coords = [c for c in ds_remap_chunked.coords if 'time' not in ds_remap_chunked[c].dims]
    if drop_coords:
        logger.info(f"Dropping non-time coords for region write: {drop_coords}")
        ds_remap_chunked = ds_remap_chunked.drop_vars(drop_coords)

    logger.info(f"Writing region to Zarr: {store_path}")
    zarr_start_time = time.time()
    logger.info(f"Region size: {ds_remap_chunked.nbytes / 1024**3:.2f} GB")

    try:
        with ProgressBar():
            ds_remap_chunked.to_zarr(
                store_path,
                region={'time': slice(i0, i1)},
                mode='r+',            # write into the existing store; never touch its metadata
                consolidated=False,   # consolidate once at the end, not once per concurrent writer
            )
    except Exception as e:
        logger.error(f"Error during region write: {e}")
        raise

    zarr_time = time.time() - zarr_start_time
    logger.info("✅ Region write completed successfully!")
    logger.info(f"Region write completed in {zarr_time/60:.1f} minutes")

    return ds_remap_chunked, zarr_time
