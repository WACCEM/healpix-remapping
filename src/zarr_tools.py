#!/usr/bin/env python3
"""
Zarr-specific utility functions for optimized data storage.

This module contains functions for:
- Zarr write progress monitoring
- Optimized Zarr writing with chunking and compression
- Zarr encoding configuration
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
