#!/usr/bin/env python3
"""
General utility functions for data processing.

This module contains helper functions for:
- Time coordinate conversion (cftime to datetime64)
- Dask client setup and configuration
- Input file searching and filtering
- Temporal averaging operations
"""

import xarray as xr
import pandas as pd
import time
import glob
import re
from datetime import datetime
from pathlib import Path
import logging
import dask
from dask.distributed import Client, LocalCluster

# Configure logging
logger = logging.getLogger(__name__)


def detect_spatial_dimensions(files, time_dim='time'):
    """
    Auto-detect spatial dimension names from dataset files.
    
    Inspects the first file to identify coordinate dimensions that are not
    the time dimension. Handles both regular grids (lat/lon) and unstructured
    grids (ncol, cell, etc.).
    
    Parameters:
    -----------
    files : list
        List of file paths to inspect (only first file is used)
    time_dim : str
        Name of the time dimension to exclude (default: 'time')
    
    Returns:
    --------
    dict : Dictionary mapping detected spatial dimension names to -1 (no chunking)
           Returns {'lat': -1, 'lon': -1} as default if detection fails
    
    Examples:
    ---------
    # IMERG/IR_IMERG: Returns {'lat': -1, 'lon': -1}
    # ERA5: Returns {'latitude': -1, 'longitude': -1}
    # E3SM: Returns {'ncol': -1}
    # MPAS: Returns {'nCells': -1}
    
    Notes:
    ------
    - Only inspects first file for efficiency
    - Sets all spatial dimensions to -1 (no chunking) for optimal remapping
    - Falls back to {'lat': -1, 'lon': -1} if detection fails
    """
    if not files:
        logger.warning("No files provided for spatial dimension detection. Using default {'lat': -1, 'lon': -1}")
        return {'lat': -1, 'lon': -1}
    
    try:
        # Open first file to inspect dimensions
        with xr.open_dataset(files[0]) as ds:
            # Find all coordinate dimensions except time
            spatial_dims = {}
            
            # Get dimensions from coordinates
            for coord_name in ds.coords:
                coord = ds.coords[coord_name]
                
                # Skip time dimension and scalar coordinates
                if coord_name == time_dim or coord.ndim == 0:
                    continue
                
                # Check if this is a 1D coordinate (spatial dimension)
                if coord.ndim == 1:
                    # Get the dimension name (usually same as coordinate name)
                    dim_name = coord.dims[0]
                    spatial_dims[dim_name] = -1
            
            # If we found spatial dimensions, return them
            if spatial_dims:
                logger.info(f"Auto-detected spatial dimensions: {list(spatial_dims.keys())}")
                return spatial_dims
            
            # Fallback: check common dimension names
            common_spatial_dims = ['lat', 'lon', 'latitude', 'longitude', 'ncol', 'cell', 'nCells', 'x', 'y']
            for dim in ds.dims:
                if dim != time_dim and dim in common_spatial_dims:
                    spatial_dims[dim] = -1
            
            if spatial_dims:
                logger.info(f"Detected spatial dimensions from common names: {list(spatial_dims.keys())}")
                return spatial_dims
            
            # Last resort: all non-time dimensions
            for dim in ds.dims:
                if dim != time_dim:
                    spatial_dims[dim] = -1
            
            if spatial_dims:
                logger.info(f"Using all non-time dimensions as spatial: {list(spatial_dims.keys())}")
                return spatial_dims
                
    except Exception as e:
        logger.warning(f"Failed to auto-detect spatial dimensions: {e}")
    
    # Ultimate fallback
    logger.info("Using default spatial dimensions: {'lat': -1, 'lon': -1}")
    return {'lat': -1, 'lon': -1}


def parse_date(date_str, is_end_date=False):
    """
    Parse date string with flexible format support.
    
    Supports multiple formats:
    - YYYY-MM-DD: Date only (hours will be set to 00:00 for start, 23:59:59 for end)
    - YYYY-MM-DDTHH: Date with hour (minutes set to 00 for start, 59 for end)
    - YYYY-MM-DD HH: Date with hour (space separator)
    
    Parameters:
    -----------
    date_str : str
        Date string to parse
    is_end_date : bool
        If True and only date provided, extend to end of day (23:59:59)
        If True and hour provided, extend to end of hour (HH:59:59)
    
    Returns:
    --------
    datetime : Parsed datetime object
    
    Examples:
    ---------
    # Start date (date only)
    >>> parse_date('2020-01-01', is_end_date=False)
    datetime.datetime(2020, 1, 1, 0, 0, 0)
    
    # End date (date only) - extends to end of day
    >>> parse_date('2020-01-03', is_end_date=True)
    datetime.datetime(2020, 1, 3, 23, 59, 59)
    
    # End date with hour - extends to end of hour
    >>> parse_date('2020-01-03T12', is_end_date=True)
    datetime.datetime(2020, 1, 3, 12, 59, 59)
    """
    # Try different formats
    formats = [
        ("%Y-%m-%dT%H", "hour_with_T"),      # 2020-01-03T23
        ("%Y-%m-%d %H", "hour_with_space"),  # 2020-01-03 23
        ("%Y-%m-%d", "date_only"),           # 2020-01-03
    ]
    
    for fmt, fmt_type in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            
            # Extend to end of period if this is an end date
            if is_end_date:
                if fmt_type == "date_only":
                    # Extend to end of day: 23:59:59
                    dt = dt.replace(hour=23, minute=59, second=59)
                elif fmt_type in ["hour_with_T", "hour_with_space"]:
                    # Extend to end of hour: HH:59:59
                    dt = dt.replace(minute=59, second=59)
            
            return dt
            
        except ValueError:
            continue
    
    # If none of the formats work, raise error with helpful message
    raise ValueError(
        f"Invalid date format: '{date_str}'\n"
        f"Supported formats:\n"
        f"  - YYYY-MM-DD (e.g., 2020-01-03)\n"
        f"  - YYYY-MM-DDTHH (e.g., 2020-01-03T23)\n"
        f"  - YYYY-MM-DD HH (e.g., 2020-01-03 23)"
    )


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


def setup_dask_client(n_workers=16, threads_per_worker=1, memory_limit=None, advanced_config=None):
    """
    Set up Dask client optimized for NERSC Perlmutter HPC system.
    
    Parameters:
    -----------
    n_workers : int
        Number of Dask workers (default: 16)
    threads_per_worker : int  
        Threads per worker (default: 1)
    memory_limit : str, optional
        Memory limit per worker. If None, auto-calculated from system memory
        (leaves 20% for OS, divides rest among workers)
    advanced_config : dict, optional
        Advanced Dask configuration options (use sparingly - defaults usually work best)
    
    Notes:
    ------
    Auto-calculated memory is usually best - prevents worker pausing/deadlocks.
    For large I/O operations: fewer workers (4-6) with more memory often works better.
    """
    
    # Auto-calculate memory limit per worker if not provided
    if memory_limit is None:
        try:
            import psutil
            total_memory_gb = psutil.virtual_memory().total / (1024**3)
            # Leave 20% for system, divide rest among workers
            usable_memory_gb = total_memory_gb * 0.8
            memory_per_worker_gb = usable_memory_gb / n_workers
            memory_limit = f"{memory_per_worker_gb:.1f}GB"
            logger.info(f"💾 Auto-calculated memory: {memory_limit}/worker from {total_memory_gb:.1f}GB total")
        except ImportError:
            memory_limit = '30GB'  # Fallback default
            logger.warning("psutil not available, using default 30GB per worker")
    
    # Minimal Dask configuration - let Dask use sensible defaults
    dask_config = {
        'distributed.worker.memory.target': 0.85,     # Start spilling at 85%
        'distributed.worker.memory.spill': 0.95,      # Spill aggressively at 95%
    }
    
    # Only apply advanced config if explicitly provided (use with caution)
    if advanced_config:
        logger.info("⚠️  Applying advanced Dask configuration - may cause issues if misconfigured")
        dask_config.update(advanced_config)
        
    dask.config.set(dask_config)
    
    # Create client with minimal settings - let Dask handle timeouts/communication
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        dashboard_address=':8787',  # Enable dashboard on port 8787
        silence_logs=True,  # Suppress Dask cluster logs
        # Let Dask use default timeout/serialization settings
    )
    client = Client(cluster)
    
    # Log basic cluster information
    logger.info(f"🚀 Dask client started: {client.dashboard_link}")
    logger.info(f"   Workers: {n_workers} × {threads_per_worker} threads = {n_workers * threads_per_worker} total threads")
    
    # Extract numeric memory for logging
    try:
        mem_value = float(memory_limit.replace('GB', ''))
        total_mem = n_workers * mem_value
        logger.info(f"   Memory: {memory_limit}/worker × {n_workers} workers = {total_mem:.1f}GB total")
    except:
        logger.info(f"   Memory: {memory_limit}/worker")
    
    logger.info(f"   Using Dask default settings for timeouts and communication")
    
    return client


def get_input_files(start_date, end_date, base_dir=None, 
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
    files = get_input_files(start_date, end_date, base_dir='/data/imerg',
                            date_pattern=r'\.(\d{8})-', date_format='%Y%m%d')
    
    # ir_imerg format: merg_2020123108_10km-pixel.nc
    files = get_input_files(start_date, end_date, base_dir='/data/ir_imerg',
                            date_pattern=r'_(\d{10})_', date_format='%Y%m%d%H',
                            use_year_subdirs=False)
    
    # Generic format: data_20200101.nc
    files = get_input_files(start_date, end_date, base_dir='/data/generic',
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


def read_concat_files(files, time_chunk_size=48, spatial_dims={'lat': -1, 'lon': -1}, 
                      max_retries=5, concat_dim='time'):
    """
    Read multiple NetCDF files and concatenate along time dimension with validation.
    
    Generic function for reading any gridded lat/lon dataset files. Handles large 
    datasets on HPC systems with retry logic for transient filesystem issues.
    
    Parameters:
    -----------
    files : list
        List of file paths to read
    time_chunk_size : int
        Time chunk size for processing (default: 48)
    spatial_dims : dict
        Spatial dimension chunking specification (default: {'lat': -1, 'lon': -1})
        Use -1 to keep dimension unchunked (full spatial grids)
        
        This parameter should be auto-detected using detect_spatial_dimensions() 
        or provided from config file for explicit control.
        
        Examples:
            {'lat': -1, 'lon': -1}  # IMERG/IR_IMERG - full spatial chunks
            {'latitude': -1, 'longitude': -1}  # ERA5 - full spatial chunks
            {'ncol': -1}  # E3SM unstructured grid
            {'lat': 100, 'lon': 100}  # Chunked spatial (for very large grids)
    max_retries : int
        Maximum number of retry attempts for file reading (default: 5)
    concat_dim : str
        Dimension along which to concatenate files (default: 'time')
        
    Returns:
    --------
    xr.Dataset : Loaded and validated dataset with files concatenated along concat_dim
    
    Raises:
    -------
    ValueError : If not all expected files can be loaded after retries
    RuntimeError : If dataset loading fails completely
    
    Notes:
    ------
    - Uses exponential backoff for retries (2s, 4s, 8s, ...)
    - Validates that number of time steps matches number of files
    - Logs detailed timing and progress information
    - Handles both cftime and standard datetime coordinates
    - For optimal performance, use detect_spatial_dimensions() to auto-detect
      the correct spatial dimension names for your dataset
    
    Examples:
    ---------
    # Default: Full spatial chunks for remapping
    ds = read_concat_files(files, time_chunk_size=24)
    
    # Auto-detected spatial dimensions (recommended)
    spatial_dims = detect_spatial_dimensions(files)
    ds = read_concat_files(files, time_chunk_size=24, spatial_dims=spatial_dims)
    
    # Custom spatial chunking
    ds = read_concat_files(files, time_chunk_size=48, 
                          spatial_dims={'lat': 100, 'lon': 100})
    
    # Unstructured grid (explicit)
    ds = read_concat_files(files, time_chunk_size=24,
                          spatial_dims={'ncol': -1})
    """
    
    retry_delay = 2  # seconds
    ds = None

    # Build chunks dictionary
    chunks = {concat_dim: time_chunk_size}
    chunks.update(spatial_dims)
    
    logger.info(f"Using chunking strategy: {chunks}")
    
    # Start timing the file reading process
    start_time = time.time()
    logger.info(f"📂 Starting to read {len(files)} files...")
    
    # Open multi-file dataset with time chunking for better parallelism
    logger.info("Opening multi-file dataset...")
    
    for attempt in range(max_retries):
        try:
            # Open with robust settings for large datasets on HPC systems
            # Use nested combine strategy which is more reliable for time series
            ds = xr.open_mfdataset(
                files,
                combine='nested',         # Better concatenation along record dimension
                concat_dim=concat_dim,    # Concatenation dimension
                compat='override',        # Handle minor metadata conflicts
                data_vars='minimal',      # Only load variables present in all files
                coords='minimal',         # Only load coordinates present in all files
                decode_times=True,
                use_cftime=True,          # Use cftime to handle various calendars
                chunks=chunks
            )
            
            logger.info(f"Dataset loaded: {ds.sizes}")
            if concat_dim in ds.dims:
                coord_values = ds[concat_dim].values
                logger.info(f"{concat_dim.capitalize()} range: {coord_values[0]} to {coord_values[-1]}")
            
            # Log timing information
            elapsed_time = time.time() - start_time
            logger.info(f"📊 File reading completed in {elapsed_time/60:.1f} minutes ({elapsed_time:.1f}s)")
            logger.info(f"📊 Reading rate: {len(files)/elapsed_time:.1f} files/second")
            
            # Validate that the number of concat steps matches expected files
            if concat_dim in ds.sizes:
                expected_steps = len(files)
                actual_steps = ds.sizes[concat_dim]
                logger.info(f"Expected {expected_steps} {concat_dim} steps, got {actual_steps}")
                
                if actual_steps < expected_steps:
                    logger.warning(f"{concat_dim.capitalize()} mismatch: expected {expected_steps} files but got {actual_steps} steps")
                    logger.warning("This may indicate file reading issues (possibly filesystem access problems)")
                    logger.warning("Some files may have failed to load properly")
                    
                    # Close the incomplete dataset before retrying
                    ds.close()
                    ds = None
                    
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying to load all files (attempt {attempt + 2}/{max_retries}) in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue  # Retry the file loading
                    else:
                        logger.error(f"All retry attempts failed - still missing {expected_steps - actual_steps} steps")
                        raise ValueError(f"Could not load all expected files after {max_retries} attempts")
                elif actual_steps > expected_steps:
                    logger.warning(f"Unexpected: got more {concat_dim} steps ({actual_steps}) than files ({expected_steps})")
                    logger.warning(f"This may indicate duplicate values or files with multiple {concat_dim} steps")
                    logger.info("✓ Proceeding with loaded data")
                    break  # Success with warning
                else:
                    logger.info(f"✓ Validation passed: all expected files loaded successfully")
                    break  # Success, exit retry loop
            else:
                logger.warning(f"Could not validate: '{concat_dim}' dimension not found in dataset")
                logger.info("✓ Proceeding with loaded data")
                break
            
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
