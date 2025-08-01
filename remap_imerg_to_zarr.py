#!/usr/bin/env python3
"""
Efficient script to remap multi-year IMERG datasets to HEALPix and save as Zarr.
Optimized for NERSC Perlmutter with Dask lazy evaluation and chunking.

Usage: python remap_imerg_to_zarr.py <start_year> <end_year> <zoom> [output_zarr_path]
Example: python remap_imerg_to_zarr.py 2019 2021 9 /pscratch/sd/w/wcmca1/GPM/IMERG_healpix_2019-2021_z9.zarr
"""

import sys
import numpy as np
import xarray as xr
import healpy as hp
import easygems.remap as egr
import dask
from dask.distributed import Client
from pathlib import Path
import glob
import logging
from datetime import datetime
import warnings
import re
import zarr
import pandas as pd
import remap_tools
import zarr_tools
import chunk_tools

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)


def convert_cftime_to_datetime64(ds):
    """
    Convert cftime coordinates to standard datetime64.
    
    This function converts cftime.DatetimeJulian objects (used by IMERG) to 
    standard numpy.datetime64, making the dataset compatible with pandas operations.
    
    Parameters:
    -----------
    ds : xarray.Dataset
        Dataset with cftime time coordinates
        
    Returns:
    --------
    xarray.Dataset : Dataset with converted time coordinates
    """
    if 'time' in ds.coords and hasattr(ds.time.values[0], 'calendar'):
        logger.info("Converting cftime coordinates to standard datetime64")
        
        # Convert via string formatting (most reliable method)
        time_strings = [t.strftime('%Y-%m-%d %H:%M:%S') for t in ds.time.values]
        new_times = pd.to_datetime(time_strings)
        
        # Create new dataset with converted times, preserving all attributes
        ds_converted = ds.assign_coords(time=new_times)
        
        # Add conversion metadata
        if 'time_conversion' not in ds_converted.attrs:
            ds_converted.attrs['time_conversion'] = 'cftime.DatetimeJulian to datetime64'
            ds_converted.attrs['original_calendar'] = ds.time.values[0].calendar
        
        logger.info(f"Converted {len(ds.time)} time coordinates from {type(ds.time.values[0]).__name__} to datetime64")
        return ds_converted
    
    return ds


def save_weights(weights, weights_file):
    """Save remapping weights to NetCDF file."""
    weights_path = Path(weights_file)
    weights_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Saving weights to {weights_path}")
    
    # Use compression for efficient storage
    encoding = {}
    for var in weights.data_vars:
        if weights[var].dtype.kind in ['i', 'u']:  # Integer variables
            encoding[var] = {'zlib': True, 'complevel': 4, 'dtype': 'int32'}
        elif weights[var].dtype.kind == 'f':  # Float variables
            encoding[var] = {'zlib': True, 'complevel': 4, 'dtype': 'float32'}
    
    weights.to_netcdf(weights_path, encoding=encoding)
    logger.info(f"Weights saved successfully ({weights_path.stat().st_size / 1024**2:.1f} MB)")


def load_weights(weights_file):
    """Load remapping weights from NetCDF file."""
    weights_path = Path(weights_file)
    if not weights_path.exists():
        raise FileNotFoundError(f"Weights file not found: {weights_path}")
    
    logger.info(f"Loading weights from {weights_path}")
    weights = xr.open_dataset(weights_path)
    
    # Print some info about the loaded weights
    if 'healpix_order' in weights.attrs:
        logger.info(f"  HEALPix order: {weights.attrs['healpix_order']}")
    if 'source_shape' in weights.attrs:
        logger.info(f"  Source grid: {weights.attrs['source_shape']}")
    if 'creation_date' in weights.attrs:
        logger.info(f"  Created: {weights.attrs['creation_date']}")
    
    return weights


def setup_dask_client(n_workers=16, threads_per_worker=8, memory_limit='30GB', advanced_config=None):
    """
    Set up Dask client optimized for NERSC Perlmutter HPC system.
    
    Parameters:
    -----------
    n_workers : int
        Number of Dask workers (optimized for Perlmutter: 16 workers for NUMA topology)
    threads_per_worker : int  
        Threads per worker (8 threads × 16 workers = 128 total, matching core count)
    memory_limit : str
        Memory limit per worker (30GB × 16 = 480GB, leaves headroom for OS)
    advanced_config : dict, optional
        Advanced Dask configuration options
    """
    import dask
    
    # Set Dask configuration for high-performance computing
    dask_config = {
        'distributed.worker.memory.target': 0.8,      # Target 80% memory usage before spilling
        'distributed.worker.memory.spill': 0.9,       # Spill at 90% memory usage
        'distributed.worker.memory.pause': 0.95,      # Pause worker at 95% memory
        'distributed.comm.timeouts.tcp': '30s',       # TCP timeout
        'distributed.comm.compression': 'lz4',        # Fast compression for network
        'array.slicing.split_large_chunks': True,     # Handle large chunks better
    }
    
    # Apply advanced configuration if provided
    if advanced_config:
        dask_config.update(advanced_config)
        
    dask.config.set(dask_config)
    
    # Create client with optimized settings
    client = Client(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        silence_logs=logging.ERROR,
        dashboard_address=':8787',  # Enable dashboard on port 8787
        # Additional performance settings
        serializers=['dask', 'pickle'],
        deserializers=['dask', 'pickle', 'error'],
    )
    
    # Log detailed cluster information
    logger.info(f"🚀 Dask client optimized for Perlmutter: {client.dashboard_link}")
    logger.info(f"   Workers: {n_workers} × {threads_per_worker} threads = {n_workers * threads_per_worker} total threads")
    logger.info(f"   Memory: {memory_limit}/worker × {n_workers} workers = {n_workers * int(memory_limit.replace('GB', ''))}GB total")
    logger.info(f"   NUMA-aware: {n_workers} workers distributed across NUMA domains")
    
    return client


def read_imerg_files(files, time_chunk_size=48, max_retries=5):
    """
    Read IMERG files into xarray dataset with validation and retry logic.
    
    Parameters:
    -----------
    files : list
        List of file paths to read
    time_chunk_size : int
        Time chunk size for processing (default: 48)
    max_retries : int
        Maximum number of retry attempts (default: 5)
        
    Returns:
    --------
    xr.Dataset : Loaded and validated dataset
    
    Raises:
    -------
    ValueError : If not all expected files can be loaded after retries
    RuntimeError : If dataset loading fails completely
    """
    import time
    
    retry_delay = 2  # seconds
    ds = None

    # Calculate optimal input chunking based on zoom level
    # Don't chunk during remapping - process full spatial grids at once
    # Only chunk time after remapping for zarr writing
    logger.info("Using full spatial chunks for remapping (no spatial chunking)")
    
    # Start timing the file reading process
    start_time = time.time()
    logger.info(f"📂 Starting to read {len(files)} IMERG files...")
    
    # Open multi-file dataset with time chunking for better parallelism
    logger.info("Opening multi-file dataset...")
    
    for attempt in range(max_retries):
        try:
            # Open with robust settings for large datasets on HPC systems
            # Use nested combine strategy which is more reliable for time series
            ds = xr.open_mfdataset(
                files,
                combine='nested',         # Better concatenation along record dimension
                concat_dim='time',        # Explicitly specify dimension for concatenation  
                compat='override',        # Handle minor metadata conflicts
                data_vars='minimal',      # Only load variables present in all files
                coords='minimal',         # Only load coordinates present in all files
                decode_times=True,
                use_cftime=True,          # Use cftime to handle Julian calendar
                chunks={'time': time_chunk_size, 'lat': -1, 'lon': -1}  # Chunk time, keep spatial intact
            )
            
            logger.info(f"Dataset loaded: {ds.sizes}")
            logger.info(f"Time range: {ds.time.min().values} to {ds.time.max().values}")
            
            # Log timing information
            elapsed_time = time.time() - start_time
            logger.info(f"📊 File reading completed in {elapsed_time/60:.1f} minutes ({elapsed_time:.1f}s)")
            logger.info(f"📊 Reading rate: {len(files)/elapsed_time:.1f} files/second")
            
            # Validate that the number of time steps matches expected files
            expected_times = len(files)
            actual_times = ds.sizes['time']
            logger.info(f"Expected {expected_times} time steps, got {actual_times}")
            
            if actual_times < expected_times:
                logger.warning(f"Time mismatch: expected {expected_times} files but got {actual_times} time steps")
                logger.warning("This may indicate file reading issues (possibly NERSC disk access problems)")
                logger.warning("Some files may have failed to load properly")
                
                # List the time gaps if possible
                if hasattr(ds.time, 'values'):
                    time_values = ds.time.values
                    logger.warning(f"Loaded time range: {time_values[0]} to {time_values[-1]}")
                
                # Close the incomplete dataset before retrying
                ds.close()
                ds = None
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying to load all files (attempt {attempt + 2}/{max_retries}) in {retry_delay} seconds...")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue  # Retry the file loading
                else:
                    logger.error(f"All retry attempts failed - still missing {expected_times - actual_times} time steps")
                    raise ValueError(f"Could not load all expected files after {max_retries} attempts")
            elif actual_times > expected_times:
                logger.warning(f"Unexpected: got more time steps ({actual_times}) than files ({expected_times})")
                logger.warning("This may indicate duplicate times or files with multiple time steps")
                logger.info("✓ Proceeding with loaded data")
                break  # Success with warning
            else:
                logger.info("✓ Time validation passed: all expected files loaded successfully")
                break  # Success, exit retry loop
            
        except Exception as e:
            logger.warning(f"Dataset loading attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if ds is not None:
                try:
                    ds.close()
                except:
                    pass
                ds = None
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("All retry attempts failed")
                raise
    
    if ds is None:
        raise RuntimeError("Failed to load dataset after all retry attempts")
    
    return ds


def temporal_average(ds, time_average, convert_time=False):
    """
    Apply temporal averaging to the dataset using xarray's resample function.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with time dimension
    time_average : str or None
        Time averaging frequency (e.g., "1h", "3h", "6h", "1d")
        If None, no averaging is applied
    convert_time : bool, optional
        If True, convert cftime coordinates to standard datetime64
        This makes the dataset compatible with pandas operations
        
    Returns:
    --------
    xr.Dataset : Temporally averaged dataset
    """
    import pandas as pd
    import cftime
    
    if time_average is None:
        logger.info("No temporal averaging requested")
        if convert_time:
            return convert_cftime_to_datetime64(ds)
        return ds
    
    logger.info(f"Applying temporal averaging: {time_average}")
    logger.info(f"Original time resolution: {ds.sizes['time']} time steps")
    logger.info(f"Original time range: {ds.time.min().values} to {ds.time.max().values}")
    
    # Check time coordinate type for logging
    first_time = ds.time.values[0]
    logger.info(f"Time coordinate type: {type(first_time)}")
    
    # Simple resample operation - let xarray handle time coordinate automatically
    ds_avg = ds.resample(time=time_average, label='left').mean(keep_attrs=True)
    
    logger.info(f"After averaging: {ds_avg.sizes['time']} time steps")
    logger.info(f"New time range: {ds_avg.time.min().values} to {ds_avg.time.max().values}")
    logger.info(f"Averaged time coordinate type: {type(ds_avg.time.values[0])}")
    
    # Convert cftime to standard datetime64 if requested
    if convert_time:
        ds_avg = convert_cftime_to_datetime64(ds_avg)
        logger.info(f"Final time coordinate type: {type(ds_avg.time.values[0])}")
    
    # Add attributes to track the averaging
    ds_avg.attrs.update({
        'temporal_averaging': time_average,
        'original_temporal_resolution': '30min',
        'processing_note': f'Temporally averaged from 30-minute to {time_average} resolution'
    })

    return ds_avg


def get_imerg_files(start_date, end_date, base_dir=None):
    """
    Get list of IMERG files for the specified date range.
    
    Parameters:
    -----------
    start_date : datetime
        Filter files from this date (inclusive)
    end_date : datetime
        Filter files to this date (inclusive)
    base_dir : str, optional
        Base directory containing yearly subdirectories
        
    Returns:
    --------
    list : Sorted list of file paths
    """
    # Extract year range from dates
    start_year = start_date.year
    end_year = end_date.year
    
    files = []
    for year in range(start_year, end_year + 1):
        year_dir = Path(base_dir) / str(year)
        if year_dir.exists():
            year_files = sorted(glob.glob(str(year_dir / "*.nc4")))
            
            # Filter files by date range
            filtered_files = []
            for file_path in year_files:
                # Extract date from filename (assuming IMERG naming convention)
                # Example: 3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4
                try:
                    filename = Path(file_path).name
                    # Look for YYYYMMDD pattern in filename
                    import re
                    date_match = re.search(r'\.(\d{8})-', filename)
                    if date_match:
                        file_date_str = date_match.group(1)
                        file_date = datetime.strptime(file_date_str, '%Y%m%d')
                        
                        # Check if file date is within range
                        if file_date < start_date or file_date > end_date:
                            continue
                        filtered_files.append(file_path)
                except:
                    # If we can't parse the date, skip the file
                    logger.warning(f"Could not parse date from filename: {filename}")
                    continue
            
            files.extend(filtered_files)
            logger.info(f"Found {len(filtered_files)} files for year {year}")
        else:
            logger.warning(f"Directory not found: {year_dir}")
    
    logger.info(f"Total files found: {len(files)}")
    return files


def gen_weights_latlon_optimized(lon, lat, zoom, weights_file=None, force_recompute=False):
    """
    Generate or load optimized remapping weights for lat/lon to HEALPix.
    
    Parameters:
    -----------
    lon : array-like
        1D longitude array in degrees
    lat : array-like
        1D latitude array in degrees  
    zoom : int
        HEALPix zoom level
    weights_file : str, optional
        Path to weights file for caching
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
        
    Returns:
    --------
    weights : xr.Dataset
        Remapping weights
    """
    # Check if we should load existing weights
    if weights_file is not None and not force_recompute:
        weights_path = Path(weights_file)
        if weights_path.exists():
            try:
                return load_weights(weights_path)
            except Exception as e:
                logger.warning(f"Error loading weights: {e}, recomputing...")
    
    logger.info(f"Computing new weights for HEALPix zoom {zoom}")
    nside = 2**zoom  # healpy uses nside = 2^order for zoom level
    npix = hp.nside2npix(nside)
    
    # Get HEALPix pixel coordinates
    hp_lon, hp_lat = hp.pix2ang(
        nside=nside, ipix=np.arange(npix), lonlat=True, nest=True
    )
    
    # Create 2D meshgrid and flatten
    lon_2d, lat_2d = np.meshgrid(lon, lat)
    source_lon = lon_2d.flatten()
    source_lat = lat_2d.flatten()
    
    # Handle global periodicity
    logger.info("Handling longitude periodicity for global grid")
    lon_extended = np.hstack([source_lon - 360, source_lon, source_lon + 360])
    lat_extended = np.tile(source_lat, 3)
    
    # Compute weights using extended grid
    weights = egr.compute_weights_delaunay(
        points=(lon_extended, lat_extended), 
        xi=(hp_lon, hp_lat)
    )
    
    # Remap source indices back to original grid size
    original_size = len(source_lon)
    weights = weights.assign(src_idx=weights.src_idx % original_size)
    
    # Add metadata
    weights.attrs.update({
        'healpix_order': zoom,
        'healpix_nside': nside,
        'healpix_npix': npix,
        'source_grid_type': 'regular_latlon',
        'source_shape': f"{len(lat)} x {len(lon)}",
        'source_lon_range': f"{np.min(lon):.3f} to {np.max(lon):.3f}",
        'source_lat_range': f"{np.min(lat):.3f} to {np.max(lat):.3f}",
        'source_resolution': f"~{np.diff(lon).mean():.4f}° x {np.diff(lat).mean():.4f}°",
        'creation_date': str(np.datetime64('now')),
        'description': 'Remapping weights from regular lat/lon grid to HEALPix'
    })
    
    # Save weights if requested
    if weights_file is not None:
        save_weights(weights, weights_file)
    
    return weights


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
    """
    import threading
    import time
    import os
    
    def progress_monitor():
        start_time = time.time()
        last_file_count = 0
        last_update_time = start_time
        
        while True:
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
                
                time.sleep(check_interval)
                
            except Exception as e:
                logger.debug(f"Progress monitor error: {e}")
                time.sleep(check_interval)
    
    # Start monitoring thread
    monitor_thread = threading.Thread(target=progress_monitor, daemon=True)
    monitor_thread.start()
    return monitor_thread


def process_imerg_to_zarr(start_date, end_date, zoom, output_zarr, 
                         weights_file=None, time_chunk_size=48,
                         input_base_dir=None, force_recompute=False, overwrite=False,
                         time_average=None, convert_time=False, dask_config=None):
    """
    Main function to process IMERG data to HEALPix Zarr following zarr_tools pattern.
    
    Parameters:
    -----------
    start_date : datetime
        Starting date (inclusive)
    end_date : datetime
        Ending date (inclusive)  
    zoom : int
        HEALPix zoom level (order)
    output_zarr : str
        Output Zarr path
    weights_file : str, optional
        Weights file path
    time_chunk_size : int
        Time chunk size for processing (default: 48 for 2 days)
    input_base_dir : str, optional
        Base directory for input files
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
    overwrite : bool, default=False
        If True, overwrite existing Zarr files; if False, error on existing files
    time_average : str, optional
        Temporal averaging frequency (e.g., "1h", "3h", "6h", "1d")
        If None, no averaging is applied
    convert_time : bool, optional
        If True, convert cftime coordinates to standard datetime64
        This makes the dataset compatible with pandas operations
    dask_config : dict, optional
        Dask configuration parameters (n_workers, threads_per_worker, memory_limit)
    overwrite : bool, default=False
        If True, overwrite existing Zarr files; if False, error on existing files
    
    Note:
    -----
    Spatial chunk size is automatically computed based on zoom level using chunk_tools.compute_chunksize()
    """
    # Setup Dask client with configuration from config file or defaults
    if dask_config:
        client = setup_dask_client(
            n_workers=dask_config.get('n_workers', 16),
            threads_per_worker=dask_config.get('threads_per_worker', 8),
            memory_limit=dask_config.get('memory_limit', '30GB'),
            advanced_config=dask_config.get('worker_options', {})
        )
    else:
        client = setup_dask_client()
    
    # Log cluster information for debugging
    logger.info(f"Dask cluster info: {len(client.scheduler_info()['workers'])} workers")
    logger.info(f"Total cluster memory: {sum(w['memory_limit'] for w in client.scheduler_info()['workers'].values()) / 1024**3:.1f} GB")
    
    # Start overall processing timer
    import time
    overall_start_time = time.time()
    
    try:
        # Get file list for date range
        base_dir = input_base_dir or "/pscratch/sd/w/wcmca1/GPM/IMERG_V07B_hpss/"
        files = get_imerg_files(start_date, end_date, base_dir)
        if not files:
            raise ValueError("No files found for the specified period")
               
        # Read files with validation and retry logic
        logger.info("🔄 Step 1: Reading IMERG files...")
        step_start = time.time()
        ds = read_imerg_files(files, time_chunk_size)
        step_time = time.time() - step_start
        logger.info(f"✅ Step 1 completed in {step_time/60:.1f} minutes")
        
        # Apply temporal averaging if requested
        logger.info("🔄 Step 2: Applying temporal averaging...")
        step_start = time.time()
        ds = temporal_average(ds, time_average, convert_time)
        step_time = time.time() - step_start
        logger.info(f"✅ Step 2 completed in {step_time:.1f} seconds")
        
        # Remap to HEALPix using remap_tools (following ICON pattern)
        logger.info("🔄 Step 3: Remapping to HEALPix...")
        step_start = time.time()
        logger.info(f"Remapping to HEALPix zoom level {zoom}")
        
        ds_remap = remap_tools.remap_delaunay(ds, zoom, weights_file, force_recompute)
        step_time = time.time() - step_start
        logger.info(f"✅ Step 3 completed in {step_time:.1f} seconds")

        logger.info(f"Remapped dataset: {ds_remap.sizes}")
        logger.info(f"Variables: {list(ds_remap.data_vars)}")

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
                import shutil
                shutil.rmtree(output_path)
            else:
                raise FileExistsError(
                    f"Zarr file already exists: {output_path}\n"
                    f"Use overwrite=True to replace it, or choose a different output path."
                )
        
        # Create encoding for efficient storage
        encoding = {
            'precipitation': {
                'compressor': zarr.Blosc(cname='zstd', clevel=3, shuffle=2),
                'chunks': (time_chunk_size, spatial_chunk_size),  # Time and spatial chunking
                'dtype': 'float32'
            },
            # Note: Do NOT specify dtype for 'time' coordinate - let xarray/zarr handle it automatically
            # Specifying 'time': {'dtype': 'datetime64[ns]'} causes time coordinate corruption
            'cell': {'dtype': 'int32'}
        }

        # Write to Zarr with progress monitoring
        logger.info("🔄 Step 4: Writing to Zarr...")
        zarr_start_time = time.time()
        logger.info("Starting Zarr write operation...")
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
        progress_monitor = monitor_zarr_write_progress(output_path, (time_chunks, spatial_chunks))
        
        # Enable Dask progress reporting for console
        from dask.diagnostics import ProgressBar, ResourceProfiler, CacheProfiler
        
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
        
        logger.info("✅ Zarr write completed successfully!")
        zarr_time = time.time() - zarr_start_time
        logger.info(f"✅ Step 4 completed in {zarr_time/60:.1f} minutes")
        
        # Overall timing summary
        total_time = time.time() - overall_start_time
        logger.info("=" * 60)
        logger.info("📈 PROCESSING SUMMARY:")
        logger.info(f"   Total processing time: {total_time/60:.1f} minutes ({total_time/3600:.1f} hours)")
        logger.info(f"   Input files: {len(files)} files")
        logger.info(f"   Processing rate: {len(files)/(total_time/60):.0f} files/minute")
        logger.info(f"   Output size: {ds_remap_chunked.nbytes / 1024**3:.1f} GB")
        logger.info(f"   Write throughput: {(ds_remap_chunked.nbytes / 1024**3)/(zarr_time/60):.1f} GB/minute")
        logger.info("=" * 60)
        
        logger.info(f"Successfully created Zarr dataset: {output_zarr}")
        logger.info(f"Final dataset shape: {ds_remap_chunked.sizes}")
        
        return ds_remap_chunked
        
    finally:
        client.close()
