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
import glob
import re
import time
from datetime import datetime
from pathlib import Path
import logging
import dask
from dask.distributed import Client

# Configure logging
logger = logging.getLogger(__name__)


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
