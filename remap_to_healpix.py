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


def process_to_healpix_zarr(start_date, end_date, zoom, output_zarr,
                           weights_file=None, overwrite=False, time_average=None,
                           preprocessing_func=None, preprocessing_kwargs=None,
                           config=None):
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
    weights_file : str, optional
        Weights file path for caching remapping weights
    overwrite : bool, default=False
        If True, overwrite existing Zarr files; if False, error on existing files
    time_average : str, optional
        Temporal averaging frequency (e.g., "1h", "3h", "6h", "1d")
        If None, no averaging is applied
    preprocessing_func : callable or list of callables, optional
        Dataset-specific preprocessing function(s) to apply after reading files.
        Can be a single function or a list of functions to apply in sequence.
    preprocessing_kwargs : dict or list of dicts, optional
        Keyword arguments to pass to preprocessing function(s)
    config : dict, optional
        Configuration dictionary containing processing parameters. If provided,
        individual parameters will be extracted from this dictionary. Required keys:
        - input_base_dir: Base directory containing data files
        - time_chunk_size: Time chunk size for processing (default: 48)
        
        Optional keys:
        - spatial_dimensions: Spatial chunking specification (default: auto-detect)
        - concat_dim: Time dimension name (default: 'time')
        - force_recompute: Force recompute weights (default: False)
        - grid_type: Grid type ('auto', 'latlon_1d', 'latlon_2d', 'unstructured')
        - convert_time: Convert cftime to datetime64 (default: False)
        - dask: Dask configuration dict (n_workers, threads_per_worker, memory_limit)
        - date_pattern: Regex pattern for date extraction (default: r'\.(\d{8})-')
        - date_format: strptime format string (default: '%Y%m%d')
        - use_year_subdirs: Use yearly subdirectories (default: True)
        - file_glob: File glob pattern (default: '*.nc*')
        - skip_variables: List of variable patterns to skip (supports wildcards)
        - required_dimensions: List of required dimension combinations
        - var_rename_map: Dict mapping input variable names to output names
    
    Returns:
    --------
    None
        Writes output to zarr file specified by output_zarr
    
    Examples:
    ---------
    # Using config dictionary (recommended)
    config = {
        'input_base_dir': '/data/SCREAM',
        'time_chunk_size': 24,
        'grid_type': 'unstructured',
        'spatial_dimensions': {'ncol': -1},
        'skip_variables': ['*_bounds', 'time_bnds'],
        'required_dimensions': [['time', 'ncol']],
        'var_rename_map': {'precip_total_surf_mass_flux': 'pr'}
    }
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        weights_file="/path/to/weights.nc",
        config=config
    )
    """
    
    # Extract parameters from config dictionary with defaults
    if config is None:
        raise ValueError("config dictionary is required")
    
    # Required parameters
    input_base_dir = config['input_base_dir']
    
    # Optional parameters with defaults
    time_chunk_size = config.get('time_chunk_size', 48)
    spatial_chunks = config.get('spatial_dimensions', None)
    concat_dim = config.get('concat_dim', 'time')
    force_recompute = config.get('force_recompute', False)
    grid_type = config.get('grid_type', 'auto')
    convert_time = config.get('convert_time', False)
    dask_config = config.get('dask', None)
    date_pattern = config.get('date_pattern', r'\.(\d{8})-')
    date_format = config.get('date_format', '%Y%m%d')
    use_year_subdirs = config.get('use_year_subdirs', True)
    file_glob = config.get('file_glob', '*.nc*')
    skip_variables = config.get('skip_variables', None)
    required_dimensions = config.get('required_dimensions', None)
    var_rename_map = config.get('var_rename_map', None)
    
    logger.info("="*70)
    logger.info("Configuration Summary")
    logger.info("="*70)
    logger.info(f"Input directory: {input_base_dir}")
    logger.info(f"Time chunk size: {time_chunk_size}")
    logger.info(f"Grid type: {grid_type}")
    if spatial_chunks:
        logger.info(f"Spatial chunks: {spatial_chunks}")
    if skip_variables:
        logger.info(f"Skip variables: {skip_variables}")
    if required_dimensions:
        logger.info(f"Required dimensions: {required_dimensions}")
    if var_rename_map:
        logger.info(f"Variable rename map: {var_rename_map}")
    logger.info("="*70)
    
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
        spatial_chunks = utilities.detect_spatial_dimensions(files, time_dim=concat_dim)
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
            spatial_dims=spatial_chunks,
            concat_dim=concat_dim
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
        
        ds_remap = remap_tools.remap_delaunay(ds, zoom, weights_file, force_recompute, grid_type,
                                              skip_variables, required_dimensions)
        step_time = time.time() - step_start
        logger.info(f"✅ Step 3 completed in {step_time:.1f} seconds")

        logger.info(f"Remapped dataset: {ds_remap.sizes}")
        logger.info(f"Variables: {list(ds_remap.data_vars)}")
        
        # Apply variable renaming if requested
        if var_rename_map:
            logger.info("🔄 Step 3b: Renaming variables...")
            step_start = time.time()
            renamed_vars = []
            for old_name, new_name in var_rename_map.items():
                if old_name in ds_remap.data_vars:
                    ds_remap = ds_remap.rename({old_name: new_name})
                    renamed_vars.append(f"{old_name} → {new_name}")
                    logger.info(f"   Renamed: {old_name} → {new_name}")
                else:
                    logger.warning(f"   Variable '{old_name}' not found in dataset, skipping rename")
            step_time = time.time() - step_start
            logger.info(f"✅ Step 3b completed in {step_time:.1f} seconds ({len(renamed_vars)} variable(s) renamed)")
            logger.info(f"Updated variables: {list(ds_remap.data_vars)}")

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
