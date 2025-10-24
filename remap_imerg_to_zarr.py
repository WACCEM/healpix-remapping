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
import time
import warnings
import logging
from src import remap_tools, utilities, zarr_tools

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)


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


def subset_time_by_minute(ds, time_subset):
    """
    Subset dataset by selecting specific minute values within each hour.
    
    This function is useful when data at different time resolutions have identical 
    values (e.g., hourly-averaged precipitation stored at 30-minute intervals).
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with time dimension
    time_subset : str
        Minute selection option:
        - '00min': Keep only times at ~00 minutes (e.g., 00:00, 01:00, 02:00)
        - '30min': Keep only times at ~30 minutes (e.g., 00:30, 01:30, 02:30)
    
    Returns:
    --------
    xr.Dataset : Dataset with subset of time steps
    
    Raises:
    -------
    ValueError : If time_subset is not '00min' or '30min'
    
    Notes:
    ------
    Uses ±5 minute tolerance to handle slight timing variations in data files.
    Handles both cftime and numpy datetime64 time coordinates.
    """
    logger.info(f"Subsetting time steps by minute value: '{time_subset}'")
    original_times = ds.sizes['time']
    
    # Extract minute values from time coordinate
    # Handle both cftime and numpy datetime64
    if hasattr(ds.time.values[0], 'minute'):
        # cftime objects
        minutes = [t.minute for t in ds.time.values]
    else:
        # numpy datetime64 - convert to pandas for minute extraction
        minutes = pd.to_datetime(ds.time.values).minute.values
    
    # Create mask based on time_subset parameter
    if time_subset == '00min':
        # Keep times close to 00 minutes (within ±5 minutes tolerance)
        mask = [abs(m) <= 5 or abs(m - 60) <= 5 for m in minutes]
        logger.info(f"Keeping time steps at approximately 00 minutes per hour")
    elif time_subset == '30min':
        # Keep times close to 30 minutes (within ±5 minutes tolerance)
        mask = [abs(m - 30) <= 5 for m in minutes]
        logger.info(f"Keeping time steps at approximately 30 minutes per hour")
    else:
        raise ValueError(f"Invalid time_subset value: '{time_subset}'. Must be '00min' or '30min'")
    
    # Apply the mask
    ds_subset = ds.isel(time=mask)
    subset_times = ds_subset.sizes['time']
    
    logger.info(f"Time steps: {original_times} → {subset_times} ({subset_times/original_times*100:.1f}% retained)")
    logger.info(f"Reduction: {original_times - subset_times} time steps removed")
    
    return ds_subset


def process_imerg_to_zarr(start_date, end_date, zoom, output_zarr, input_base_dir,
                         weights_file=None, time_chunk_size=48,
                         force_recompute=False, overwrite=False,
                         time_average=None, convert_time=False, dask_config=None,
                         date_pattern=r'\.(\d{8})-', date_format='%Y%m%d',
                         use_year_subdirs=True, file_glob='*.nc*',
                         time_subset=None):
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
    time_subset : str, optional
        Subset time steps to reduce output size (useful when data at different 
        time resolutions have identical values). Options:
        - None (default): Keep all time steps
        - '00min': Keep only time steps at 00 minutes (e.g., 00:00, 01:00, 02:00)
        - '30min': Keep only time steps at 30 minutes (e.g., 00:30, 01:30, 02:30)
        This reduces output by ~50% when hourly-averaged data is stored at 30-min intervals.
        Note: Uses minute values, so 00:01 or 00:29 will match '00min' if close enough.
    
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
    
    # IR_IMERG with time subset (reduce output by 50%)
    # Useful when precipitation is hourly-averaged but stored at 30-min intervals
    process_imerg_to_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        zoom=9,
        output_zarr="/path/to/output_00min.zarr",
        input_base_dir="/data/ir_imerg",
        date_pattern=r'_(\d{10})_',
        date_format='%Y%m%d%H',
        use_year_subdirs=True,
        file_glob='merg_*.nc',
        time_subset='00min'  # Keep only times at 00, 01, 02, ... hours
    )
    
    Note:
    -----
    Spatial chunk size is automatically computed based on zoom level using chunk_tools.compute_chunksize()
    """
    # Setup Dask client with configuration from config file or defaults
    if dask_config:
        client = utilities.setup_dask_client(
            n_workers=dask_config.get('n_workers', 16),
            threads_per_worker=dask_config.get('threads_per_worker', 8),
            memory_limit=dask_config.get('memory_limit', '30GB'),
            advanced_config=dask_config.get('worker_options', {})
        )
    else:
        client = utilities.setup_dask_client()
    
    # Log cluster information for debugging
    logger.info(f"Dask cluster info: {len(client.scheduler_info()['workers'])} workers")
    logger.info(f"Total cluster memory: {sum(w['memory_limit'] for w in client.scheduler_info()['workers'].values()) / 1024**3:.1f} GB")
    
    # Start overall processing timer
    overall_start_time = time.time()
    
    try:
        # Get file list for date range
        files = utilities.get_input_files(
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
        
        # Apply time subsetting if requested (before temporal averaging)
        if time_subset is not None:
            logger.info("🔄 Step 1b: Applying time subset...")
            step_start = time.time()
            ds = subset_time_by_minute(ds, time_subset)
            step_time = time.time() - step_start
            logger.info(f"✅ Step 1b completed in {step_time:.1f} seconds")
        
        # Apply temporal averaging if requested
        logger.info("🔄 Step 2: Applying temporal averaging...")
        step_start = time.time()
        ds = utilities.temporal_average(ds, time_average, convert_time)
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
        ds_remap_chunked, zarr_time = zarr_tools.write_zarr_with_monitoring(
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
