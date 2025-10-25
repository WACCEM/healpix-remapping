#!/usr/bin/env python3
r"""
Efficient script to remap gridded lat/lon datasets to HEALPix and save as Zarr.
Optimized for NERSC Perlmutter with Dask lazy evaluation and chunking.

This generalized pipeline works with any gridded lat/lon NetCDF dataset, providing
flexible file pattern matching and optional dataset-specific preprocessing.

Pipeline Overview:
1. Read NetCDF files from input directory (with flexible file pattern matching)
2. Apply optional dataset-specific preprocessing (e.g., time subsetting for IMERG)
3. Apply temporal averaging (e.g., 30min → 1h, optional)
4. Remap from lat/lon to HEALPix grid using Delaunay triangulation
5. Write optimized Zarr output with compression and chunking

Usage: 
    This is a library module - import and use process_to_healpix_zarr() function
    
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
    spatial_chunks : dict - Spatial chunking (default: {'lat': -1, 'lon': -1})
    time_average : str - Temporal averaging (e.g., "1h", "3h", "6h", "1d")
    convert_time : bool - Convert cftime to datetime64 (default: False)
    overwrite : bool - Overwrite existing files (default: False)
    dask_config : dict - Dask configuration parameters
    preprocessing_func : callable or list - Dataset-specific preprocessing function(s)
    preprocessing_kwargs : dict or list of dicts - Arguments for preprocessing functions

Examples:
    
    # Example 1: Original IMERG data
    from datetime import datetime
    from remap_to_healpix import process_to_healpix_zarr
    
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
        zoom=9,
        output_zarr="/path/to/imerg_output.zarr",
        input_base_dir="/path/to/IMERG_data",
        time_average="1h",
        convert_time=True,
        overwrite=True
    )
    
    # Example 2: IR_IMERG data with time subsetting
    # Filename: merg_2020123108_10km-pixel.nc
    from src.preprocessing import subset_time_by_minute
    
    process_to_healpix_zarr(
        start_date=datetime(2020, 12, 31, 8),
        end_date=datetime(2020, 12, 31, 18),
        zoom=9,
        output_zarr="/path/to/ir_imerg_output.zarr",
        input_base_dir="/path/to/ir_imerg_data",
        date_pattern=r'_(\d{10})_',
        date_format='%Y%m%d%H',
        use_year_subdirs=False,
        file_glob='*.nc',
        preprocessing_func=subset_time_by_minute,
        preprocessing_kwargs={'time_subset': '00min'},
        convert_time=True,
        overwrite=True
    )
    
    # Example 3: Generic dataset (e.g., simulation, reanalysis)
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        zoom=9,
        output_zarr="/path/to/generic_output.zarr",
        input_base_dir="/path/to/data",
        date_pattern=r'_(\d{8})\.nc',
        date_format='%Y%m%d',
        use_year_subdirs=False,
        file_glob='precip_*.nc',
        spatial_chunks={'lat': 100, 'lon': 100},  # Custom spatial chunking
        time_average="1d",  # Daily averaging
        overwrite=True
    )

See FILE_PATTERN_GUIDE.md for more examples and pattern configuration help.

Backward Compatibility:
    The old function name process_imerg_to_zarr() is maintained as an alias
    for backwards compatibility.
"""

import time
import warnings
import logging
from src import remap_tools, utilities, zarr_tools, preprocessing

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)


def process_to_healpix_zarr(start_date, end_date, zoom, output_zarr, input_base_dir,
                           weights_file=None, time_chunk_size=48,
                           spatial_chunks=None,
                           force_recompute=False, overwrite=False,
                           time_average=None, convert_time=False, dask_config=None,
                           date_pattern=r'\.(\d{8})-', date_format='%Y%m%d',
                           use_year_subdirs=True, file_glob='*.nc*',
                           preprocessing_func=None, preprocessing_kwargs=None):
    r"""
    Main function to process gridded lat/lon datasets to HEALPix Zarr format.
    
    Generalized pipeline that works with any gridded lat/lon NetCDF dataset,
    with optional dataset-specific preprocessing (e.g., time subsetting for IMERG).
    
    Spatial dimensions are auto-detected from the first data file, but can be
    overridden via the spatial_chunks parameter for explicit control.
    
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
    spatial_chunks : dict, optional
        Spatial dimension chunking specification. If None, dimensions will be
        auto-detected from the first data file.
        
        Auto-detection handles:
            - IMERG/IR_IMERG: {'lat': -1, 'lon': -1}
            - ERA5: {'latitude': -1, 'longitude': -1}
            - E3SM: {'ncol': -1}
            - MPAS: {'nCells': -1}
        
        Override examples:
            {'lat': -1, 'lon': -1}  # Full spatial chunks (recommended for remapping)
            {'lat': 100, 'lon': 100}  # Chunked spatial (for very large grids)
            {'latitude': -1, 'longitude': -1}  # ERA5 explicit
            {'ncol': -1}  # Unstructured grid explicit
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
            IR_IMERG: r'_(\d{10})_' matches '_2020123108_'
    date_format : str, optional
        strptime format string to parse the date (default: '%Y%m%d')
        Examples:
            IMERG: '%Y%m%d' for YYYYMMDD
            IR_IMERG: '%Y%m%d%H' for YYYYMMDDhh
    use_year_subdirs : bool, optional
        If True, search in yearly subdirectories (default: True)
        If False, search directly in input_base_dir
    file_glob : str, optional
        Glob pattern for file matching (default: '*.nc*')
    preprocessing_func : callable or list of callables, optional
        Dataset-specific preprocessing function(s) to apply after reading files.
        Can be a single function or a list of functions to apply in sequence.
        Each function should accept an xarray Dataset as first argument and return
        a modified Dataset.
        Examples:
            preprocessing.subset_time_by_minute
            [preprocessing.subset_time_by_minute, preprocessing.apply_quality_mask]
    preprocessing_kwargs : dict or list of dicts, optional
        Keyword arguments to pass to preprocessing_func. If preprocessing_func is
        a list, this should be a list of dicts with the same length.
        Examples:
            {'time_subset': '00min'}
            [{'time_subset': '00min'}, {'quality_threshold': 0.8}]
    
    Returns:
    --------
    xr.Dataset : The processed and remapped dataset
    
    Examples:
    ---------
    # IMERG precipitation data (auto-detect dimensions)
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        input_base_dir="/data/IMERG",
        time_average="1h"
    )
    
    # ERA5 with explicit spatial dimensions
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        input_base_dir="/data/ERA5",
        spatial_chunks={'latitude': -1, 'longitude': -1}  # Override auto-detection
    )
    
    # IR_IMERG with time subsetting (new flexible approach)
    from src.preprocessing import subset_time_by_minute
    
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        input_base_dir="/data/ir_imerg",
        date_pattern=r'_(\d{10})_',
        date_format='%Y%m%d%H',
        use_year_subdirs=True,
        file_glob='merg_*.nc',
        preprocessing_func=subset_time_by_minute,
        preprocessing_kwargs={'time_subset': '00min'}
    )
    
    # Multiple preprocessing steps (chained preprocessing)
    from src.preprocessing import subset_time_by_minute, apply_quality_mask
    
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        input_base_dir="/data/satellite",
        preprocessing_func=[subset_time_by_minute, apply_quality_mask],
        preprocessing_kwargs=[
            {'time_subset': '00min'},
            {'quality_threshold': 0.8}
        ]
    )
    
    # Generic dataset with custom spatial chunking
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        input_base_dir="/data/generic",
        date_pattern=r'_(\d{8})\.nc',
        date_format='%Y%m%d',
        use_year_subdirs=False,
        file_glob='data_*.nc',
        spatial_chunks={'lat': 200, 'lon': 200},  # Custom chunking for large grids
        time_average="1d"
    )
    
    Note:
    -----
    This is the generalized version. The old function name process_imerg_to_zarr()
    is maintained as an alias for backwards compatibility.
    
    Spatial Dimension Detection (Hybrid Approach):
    -----------------------------------------------
    1. If spatial_chunks is provided → use it (explicit override)
    2. Otherwise, auto-detect from first data file
    3. Fallback to {'lat': -1, 'lon': -1} if detection fails
    
    This hybrid approach provides flexibility while being user-friendly.
    """
    
    # Get file list for date range FIRST (needed for auto-detection)
    files = utilities.get_input_files(
        start_date, end_date, input_base_dir,
        date_pattern=date_pattern,
        date_format=date_format,
        use_year_subdirs=use_year_subdirs,
        file_glob=file_glob
    )
    if not files:
        raise ValueError("No files found for the specified period")
    
    # Hybrid spatial dimension detection:
    # Priority: explicit config > auto-detection > default fallback
    if spatial_chunks is None:
        logger.info("🔍 Spatial dimensions not specified - auto-detecting from first file...")
        spatial_chunks = utilities.detect_spatial_dimensions(files)
    else:
        logger.info(f"✅ Using explicitly provided spatial dimensions: {spatial_chunks}")
    
    # Setup Dask client with configuration from config file or defaults
    if dask_config:
        client = utilities.setup_dask_client(
            n_workers=dask_config.get('n_workers'),
            threads_per_worker=dask_config.get('threads_per_worker'),
            memory_limit=dask_config.get('memory_limit'),
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
        # Read files with validation and retry logic
        logger.info("🔄 Step 1: Reading files and concatenating along time dimension...")
        step_start = time.time()
        ds = utilities.read_concat_files(
            files, 
            time_chunk_size=time_chunk_size,
            spatial_dims=spatial_chunks
        )
        step_time = time.time() - step_start
        logger.info(f"✅ Step 1 completed in {step_time/60:.1f} minutes")
        
        # Apply dataset-specific preprocessing if requested
        if preprocessing_func is not None:
            logger.info("🔄 Step 1b: Applying dataset-specific preprocessing...")
            step_start = time.time()
            
            # Handle single function or list of functions
            funcs = preprocessing_func if isinstance(preprocessing_func, list) else [preprocessing_func]
            kwargs_list = preprocessing_kwargs if isinstance(preprocessing_kwargs, list) else [preprocessing_kwargs or {}]
            
            # Ensure kwargs_list matches funcs length
            if len(kwargs_list) == 1 and len(funcs) > 1:
                kwargs_list = kwargs_list * len(funcs)
            elif len(kwargs_list) != len(funcs):
                raise ValueError(f"preprocessing_kwargs length ({len(kwargs_list)}) must match preprocessing_func length ({len(funcs)})")
            
            # Apply preprocessing functions in sequence
            for i, (func, kwargs) in enumerate(zip(funcs, kwargs_list)):
                func_name = getattr(func, '__name__', str(func))
                logger.info(f"   Applying preprocessing step {i+1}/{len(funcs)}: {func_name}")
                ds = func(ds, **kwargs)
            
            step_time = time.time() - step_start
            logger.info(f"✅ Step 1b completed in {step_time:.1f} seconds ({len(funcs)} function(s) applied)")
        
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


# Backwards compatibility: maintain old function name as alias
def process_imerg_to_zarr(*args, **kwargs):
    """
    Backwards compatibility alias for process_to_healpix_zarr().
    
    This function name is deprecated but maintained for backwards compatibility.
    Please use process_to_healpix_zarr() for new code.
    """
    return process_to_healpix_zarr(*args, **kwargs)
