#!/usr/bin/env python3
r"""
Efficient script to remap gridded lat/lon datasets to HEALPix and save as Zarr.
Optimized for NERSC Perlmutter with Dask lazy evaluation and chunking.

Originally developed for IMERG precipitation data, this script has been generalized
to work with any gridded lat/lon NetCDF dataset.

Pipeline Overview:
1. Read NetCDF files from input directory (with flexible file pattern matching)
2. Apply temporal averaging (e.g., 30min → 1h, optional)
3. Remap from lat/lon to HEALPix grid using Delaunay triangulation
4. Write optimized Zarr output with compression and chunking

Usage: 
    This is a library module - import and use process_imerg_to_zarr() function
    
Required Parameters:
    start_date : datetime - Starting date (inclusive)
    end_date : datetime - Ending date (inclusive)  
    zoom : int - HEALPix zoom level (order)
    output_zarr : str - Output Zarr path
    input_base_dir : str - Base directory containing data files

File Pattern Parameters:
    date_pattern : str - Regex to extract date from filename (default: r'\.(\d{8})-')
    date_format : str - strptime format for parsing (default: '%Y%m%d')
    use_year_subdirs : bool - Search yearly subdirs (default: True)
    file_glob : str - File matching pattern (default: '*.nc*')

Optional Parameters:
    weights_file : str - Path for caching remapping weights
    time_chunk_size : int - Time chunk size (default: 48)
    time_average : str - Temporal averaging (e.g., "1h", "3h", "6h", "1d")
    convert_time : bool - Convert cftime to datetime64 (default: False)
    overwrite : bool - Overwrite existing files (default: False)
    dask_config : dict - Dask configuration parameters

Examples:
    
    # Example 1: Original IMERG data (default settings)
    from datetime import datetime
    from remap_imerg_to_zarr import process_imerg_to_zarr
    
    process_imerg_to_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
        zoom=9,
        output_zarr="/path/to/imerg_output.zarr",
        input_base_dir="/path/to/IMERG_data",
        time_average="1h",
        convert_time=True,
        overwrite=True
    )
    
    # Example 2: ir_imerg data (hourly files, flat directory)
    # Filename: merg_2020123108_10km-pixel.nc
    process_imerg_to_zarr(
        start_date=datetime(2020, 12, 31, 8),
        end_date=datetime(2020, 12, 31, 18),
        zoom=9,
        output_zarr="/path/to/ir_imerg_output.zarr",
        input_base_dir="/path/to/ir_imerg_data",
        date_pattern=r'_(\d{10})_',      # YYYYMMDDhh pattern
        date_format='%Y%m%d%H',          # Parse with hour
        use_year_subdirs=False,          # Flat directory structure
        file_glob='*.nc',
        convert_time=True,
        overwrite=True
    )

See FILE_PATTERN_GUIDE.md for more examples and pattern configuration help.
"""

import xarray as xr
import pandas as pd
import zarr
import glob
import shutil
import re
import time
from datetime import datetime
from pathlib import Path
import warnings
import logging
import threading
import dask
from dask.distributed import Client
from dask.diagnostics import ProgressBar, ResourceProfiler, CacheProfiler
from src import remap_tools, chunk_tools

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


def get_imerg_files(start_date, end_date, base_dir=None, 
                   date_pattern=r'\.(\d{8})-', date_format='%Y%m%d',
                   use_year_subdirs=True, file_glob='*.nc*'):
    r"""
    Get list of files for the specified date range with flexible date pattern matching.
    
    This function searches for files containing date information in their filenames
    and filters them based on the specified date range. It supports different
    filename conventions through customizable regex patterns and date formats.
    
    Parameters:
    -----------
    start_date : datetime
        Filter files from this date (inclusive)
    end_date : datetime
        Filter files to this date (inclusive)
    base_dir : str, optional
        Base directory containing data files or yearly subdirectories
    date_pattern : str, optional
        Regex pattern to extract date string from filename. Use parentheses to 
        capture the date string group. Default: r'\.(\d{8})-' for IMERG format.
        Examples:
            IMERG: r'\.(\d{8})-' matches '.20200101-' 
            ir_imerg: r'_(\d{10})_' matches '_2020123108_' (YYYYMMDDhh)
            Generic: r'(\d{8})' matches any 8-digit sequence
    date_format : str, optional
        strptime format string to parse the captured date string.
        Default: '%Y%m%d' for YYYYMMDD format.
        Examples:
            IMERG: '%Y%m%d' for YYYYMMDD
            ir_imerg: '%Y%m%d%H' for YYYYMMDDhh
            With separator: '%Y-%m-%d' for YYYY-MM-DD
    use_year_subdirs : bool, optional
        If True, search in yearly subdirectories (base_dir/YYYY/).
        If False, search directly in base_dir. Default: True
    file_glob : str, optional
        Glob pattern for file matching. Default: '*.nc*'
        
    Returns:
    --------
    list : Sorted list of file paths matching the date range
    
    Examples:
    ---------
    # IMERG format: 3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4
    files = get_imerg_files(start_date, end_date, base_dir='/data/imerg',
                           date_pattern=r'\.(\d{8})-', date_format='%Y%m%d')
    
    # ir_imerg format: merg_2020123108_10km-pixel.nc
    files = get_imerg_files(start_date, end_date, base_dir='/data/ir_imerg',
                           date_pattern=r'_(\d{10})_', date_format='%Y%m%d%H',
                           use_year_subdirs=False)
    
    # Generic format: data_20200101.nc
    files = get_imerg_files(start_date, end_date, base_dir='/data/generic',
                           date_pattern=r'_(\d{8})\.', date_format='%Y%m%d',
                           use_year_subdirs=False)
    """
    # Extract year range from dates
    start_year = start_date.year
    end_year = end_date.year
    
    # Compile regex pattern for efficiency
    try:
        date_regex = re.compile(date_pattern)
    except re.error as e:
        raise ValueError(f"Invalid regex pattern '{date_pattern}': {e}")
    
    files = []
    
    if use_year_subdirs:
        # Search in yearly subdirectories
        for year in range(start_year, end_year + 1):
            year_dir = Path(base_dir) / str(year)
            if year_dir.exists():
                year_files = sorted(glob.glob(str(year_dir / file_glob)))
                
                # Filter files by date range
                filtered_files = filter_files_by_date(
                    year_files, start_date, end_date, 
                    date_regex, date_format
                )
                
                files.extend(filtered_files)
                logger.info(f"Found {len(filtered_files)} files for year {year}")
            else:
                logger.warning(f"Directory not found: {year_dir}")
    else:
        # Search directly in base directory
        base_path = Path(base_dir)
        if base_path.exists():
            all_files = sorted(glob.glob(str(base_path / file_glob)))
            
            # Filter files by date range
            filtered_files = filter_files_by_date(
                all_files, start_date, end_date,
                date_regex, date_format
            )
            
            files.extend(filtered_files)
            logger.info(f"Found {len(filtered_files)} files in {base_dir}")
        else:
            logger.warning(f"Directory not found: {base_dir}")
    
    logger.info(f"Total files found: {len(files)}")
    
    if len(files) == 0:
        logger.warning(f"No files found matching pattern '{date_pattern}' in date range {start_date} to {end_date}")
        logger.warning(f"Check that date_pattern and date_format are correct for your filenames")
    
    return files


def filter_files_by_date(file_list, start_date, end_date, date_regex, date_format):
    """
    Filter a list of files based on dates extracted from filenames.
    
    Parameters:
    -----------
    file_list : list
        List of file paths to filter
    start_date : datetime
        Starting date (inclusive)
    end_date : datetime
        Ending date (inclusive)
    date_regex : re.Pattern
        Compiled regex pattern to extract date string
    date_format : str
        strptime format string to parse the date
        
    Returns:
    --------
    list : Filtered list of file paths within date range
    """
    filtered_files = []
    skipped_count = 0
    
    for file_path in file_list:
        filename = Path(file_path).name
        
        try:
            # Extract date string using regex
            date_match = date_regex.search(filename)
            
            if date_match:
                # Get the first captured group (the date string)
                file_date_str = date_match.group(1)
                
                # Parse the date string
                file_date = datetime.strptime(file_date_str, date_format)
                
                # Check if file date is within range
                if start_date <= file_date <= end_date:
                    filtered_files.append(file_path)
                else:
                    skipped_count += 1
            else:
                logger.debug(f"No date match in filename: {filename}")
                skipped_count += 1
                
        except ValueError as e:
            # Date parsing error
            logger.warning(f"Could not parse date from filename '{filename}': {e}")
            skipped_count += 1
        except Exception as e:
            # Other errors
            logger.warning(f"Error processing filename '{filename}': {e}")
            skipped_count += 1
    
    if skipped_count > 0:
        logger.debug(f"Skipped {skipped_count} files (outside date range or unparseable)")
    
    return filtered_files


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
    progress_monitor = monitor_zarr_write_progress(output_path, (time_chunks, spatial_chunks))
    
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
    
    zarr_time = time.time() - zarr_start_time
    logger.info("✅ Zarr write completed successfully!")
    logger.info(f"Zarr write completed in {zarr_time/60:.1f} minutes")
    
    return ds_remap_chunked, zarr_time


def process_imerg_to_zarr(start_date, end_date, zoom, output_zarr, input_base_dir,
                         weights_file=None, time_chunk_size=48,
                         force_recompute=False, overwrite=False,
                         time_average=None, convert_time=False, dask_config=None,
                         date_pattern=r'\.(\d{8})-', date_format='%Y%m%d',
                         use_year_subdirs=True, file_glob='*.nc*'):
    r"""
    Main function to process gridded data to HEALPix Zarr with optimized processing pipeline.
    
    This function was originally designed for IMERG data but has been generalized to 
    work with any gridded lat/lon NetCDF dataset.
    
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
    input_base_dir : str
        Base directory containing data files or yearly subdirectories (required)
    weights_file : str, optional
        Weights file path for caching remapping weights
    time_chunk_size : int
        Time chunk size for processing (default: 48 for 2 days)
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
    date_pattern : str, optional
        Regex pattern to extract date string from filename (default: r'\.(\d{8})-')
        Examples:
            IMERG: r'\.(\d{8})-' matches '.20200101-'
            ir_imerg: r'_(\d{10})_' matches '_2020123108_'
    date_format : str, optional
        strptime format string to parse the date (default: '%Y%m%d')
        Examples:
            IMERG: '%Y%m%d' for YYYYMMDD
            ir_imerg: '%Y%m%d%H' for YYYYMMDDhh
    use_year_subdirs : bool, optional
        If True, search in yearly subdirectories (default: True for IMERG)
        If False, search directly in input_base_dir
    file_glob : str, optional
        Glob pattern for file matching (default: '*.nc*')
    
    Returns:
    --------
    xr.Dataset : The processed and remapped dataset
    
    Examples:
    ---------
    # IMERG data (original use case)
    process_imerg_to_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        input_base_dir="/data/IMERG",
        time_average="1h"
    )
    
    # ir_imerg data (new format)
    process_imerg_to_zarr(
        start_date=datetime(2020, 12, 31, 8),
        end_date=datetime(2020, 12, 31, 18),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        input_base_dir="/data/ir_imerg",
        date_pattern=r'_(\d{10})_',
        date_format='%Y%m%d%H',
        use_year_subdirs=False,
        file_glob='*.nc'
    )
    
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
    overall_start_time = time.time()
    
    try:
        # Get file list for date range
        files = get_imerg_files(
            start_date, end_date, input_base_dir,
            date_pattern=date_pattern,
            date_format=date_format,
            use_year_subdirs=use_year_subdirs,
            file_glob=file_glob
        )
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

        # Write to Zarr with optimal chunking and monitoring
        logger.info("🔄 Step 4: Writing to Zarr...")
        step_start = time.time()
        ds_remap_chunked, zarr_time = write_zarr_with_monitoring(
            ds_remap, output_zarr, time_chunk_size, zoom, overwrite
        )
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
        # Graceful shutdown of Dask client
        try:
            logger.info("Shutting down Dask cluster...")
            client.shutdown()  # Gracefully shutdown workers first
        except Exception as e:
            logger.debug(f"Error during client shutdown: {e}")
        finally:
            try:
                client.close()  # Then close the client
            except Exception as e:
                logger.debug(f"Error during client close: {e}")
