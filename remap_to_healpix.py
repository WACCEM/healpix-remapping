#!/usr/bin/env python3
r"""
Efficient script to remap gridded datasets to HEALPix and save as Zarr.
Optimized for NERSC Perlmutter with Dask lazy evaluation and chunking.

This generalized pipeline works with any gridded NetCDF dataset, supporting both
regular lat/lon grids and unstructured grids (e.g., SCREAM, E3SM), providing
flexible file pattern matching and optional dataset-specific preprocessing.

Pipeline Overview:
1. Read NetCDF files from input directory (with flexible file pattern matching)
2. Apply optional dataset-specific preprocessing (e.g., time subsetting)
3. Apply temporal averaging (e.g., 30min → 1h, optional)
4. Remap to HEALPix grid (auto-detects grid type: regular or unstructured)
5. Apply variable subsetting and renaming (optional)
6. Write optimized Zarr output with compression and chunking

Usage: 
    This is a library module - import and use process_to_healpix_zarr() function
    
Function Signature:
    process_to_healpix_zarr(
        start_date, end_date, zoom, output_zarr,
        weights_file=None, overwrite=False, time_average=None,
        preprocessing_func=None, preprocessing_kwargs=None,
        config=None
    )

Required Parameters:
    start_date : datetime - Starting date (inclusive)
    end_date : datetime - Ending date (inclusive)  
    zoom : int - HEALPix zoom level (order)
    output_zarr : str - Output Zarr path
    config : dict - Configuration dictionary (see below)

Configuration Dictionary (config) - Required Keys:
    input_base_dir : str - Base directory containing data files
    
Configuration Dictionary (config) - Optional Keys:
    # Grid and coordinate configuration
    grid_type : str - Grid type ('auto', 'latlon_1d', 'latlon_2d', 'unstructured')
    x_dimname : str - X dimension name (e.g., 'lon', 'ncol')
    y_dimname : str - Y dimension name (e.g., 'lat')
    x_coordname : str - X coordinate variable name
    y_coordname : str - Y coordinate variable name
    spatial_dimensions : dict - Spatial chunking (e.g., {'lat': -1, 'lon': -1})
    
    # File pattern matching
    date_pattern : str - Regex to extract date from filename
    date_format : str - strptime format for parsing dates
    use_year_subdirs : bool - Search yearly subdirectories
    file_glob : str - File matching pattern
    
    # Variable selection and renaming
    remap_variables : dict - Map input names to output names (e.g., {'u': 'ua'})
    passthrough_variables : list - Variables to keep without remapping
    skip_variables : list - Variable patterns to skip (supports wildcards)
    required_dimensions : list - Required dimension combinations
    
    # Processing parameters
    time_chunk_size : int - Time chunk size (default: 48)
    concat_dim : str - Time dimension name (default: 'time')
    convert_time : bool - Convert cftime to datetime64 (default: False)
    force_recompute : bool - Force recompute weights (default: False)
    
    # Dask configuration
    dask : dict - Dask config (n_workers, threads_per_worker, memory_limit)

Optional Function Parameters:
    weights_file : str - Path for caching remapping weights
    time_average : str - Temporal averaging (e.g., "1h", "3h", "6h", "1d")
    overwrite : bool - Overwrite existing files (default: False)
    preprocessing_func : callable or list - Dataset-specific preprocessing function(s)
    preprocessing_kwargs : dict or list - Arguments for preprocessing functions

Examples:
    
    # Example 1: IMERG data (regular lat/lon grid)
    from datetime import datetime
    from remap_to_healpix import process_to_healpix_zarr
    
    config = {
        'input_base_dir': '/path/to/IMERG_data',
        'time_chunk_size': 24,
        'convert_time': True,
        'date_pattern': r'\.(\d{8})-',
        'date_format': '%Y%m%d',
        'use_year_subdirs': True,
        'file_glob': '3B-HHR.MS.MRG.3IMERG.*.nc4'
    }
    
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
        zoom=9,
        output_zarr="/path/to/imerg_output.zarr",
        weights_file="/path/to/weights/imerg_z9_weights.nc",
        time_average="1h",
        overwrite=True,
        config=config
    )
    
    # Example 2: SCREAM data (unstructured grid)
    config = {
        'input_base_dir': '/path/to/SCREAM_data',
        'time_chunk_size': 24,
        'grid_type': 'unstructured',
        'x_dimname': 'ncol',
        'x_coordname': 'lon',
        'y_coordname': 'lat',
        'spatial_dimensions': {'ncol': -1},
        'date_pattern': r'\.(\d{4}-\d{2}-\d{2})-',
        'date_format': '%Y-%m-%d',
        'use_year_subdirs': False,
        'file_glob': '*.eam.h0.*.nc',
        'remap_variables': {
            'u': 'ua',
            'v': 'va',
            'T': 'ta',
            'Q': 'hus'
        },
        'passthrough_variables': ['hyam', 'hybm', 'P0'],
        'skip_variables': ['*_bounds', 'time_bnds']
    }
    
    process_to_healpix_zarr(
        start_date=datetime(2019, 9, 1),
        end_date=datetime(2019, 9, 30),
        zoom=9,
        output_zarr="/path/to/scream_output.zarr",
        weights_file="/path/to/weights/scream_ne1024_z9_weights.nc",
        overwrite=True,
        config=config
    )
    
    # Example 3: IR_IMERG data with preprocessing
    from src.preprocessing import subset_time_by_minute
    
    config = {
        'input_base_dir': '/path/to/ir_imerg_data',
        'time_chunk_size': 24,
        'convert_time': True,
        'date_pattern': r'_(\d{10})_',
        'date_format': '%Y%m%d%H',
        'use_year_subdirs': True,
        'file_glob': 'merg_*.nc'
    }
    
    process_to_healpix_zarr(
        start_date=datetime(2020, 12, 31, 8),
        end_date=datetime(2020, 12, 31, 18),
        zoom=9,
        output_zarr="/path/to/ir_imerg_output.zarr",
        weights_file="/path/to/weights/ir_imerg_z9_weights.nc",
        preprocessing_func=subset_time_by_minute,
        preprocessing_kwargs={'time_subset': '00min'},
        overwrite=True,
        config=config
    )

See README.md for more configuration examples and detailed usage instructions.

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
        - remap_variables: Dict mapping input variable names to output names
        - input_files: Pre-searched files (dict or list), bypasses get_input_files()
          Dict format for multi-variable: {'var1': [files], 'var2': [files]}
          List format for single/all variables: [file1, file2, ...]
        - combine_vars: Merge variables from separate files (default: False)
          Required for ERA5 multi-variable workflow
    
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
        'remap_variables': {'precip_total_surf_mass_flux': 'pr'}
    }
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        zoom=9,
        output_zarr="/path/to/output.zarr",
        weights_file="/path/to/weights.nc",
        config=config
    )
    
    # Example: ERA5 data with multi-variable files
    from src.utilities import get_era5_input_files
    
    file_dict = get_era5_input_files(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 7),
        variables=['T', 'Q', 'U', 'V'],
        config=era5_config
    )
    
    config = {
        'input_files': file_dict,  # Pre-searched files by variable
        'combine_vars': True,       # Merge variables into one dataset
        'remap_variables': {'T': 'ta', 'Q': 'hus'},
        'time_chunk_size': 24,
        'grid_type': 'latlon_1d'
    }
    
    process_to_healpix_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 7),
        zoom=8,
        output_zarr="/path/to/era5_output.zarr",
        weights_file="/path/to/era5_weights.nc",
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
    remap_variables = config.get('remap_variables', None)
    input_files = config.get('input_files', None)  # Pre-searched files (ERA5)
    combine_vars = config.get('combine_vars', False)  # Merge multi-variable files
    
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
    if remap_variables:
        logger.info(f"Remap variables: {remap_variables}")
    logger.info("="*70)
    
    # Get file list for date range FIRST (needed for auto-detection)
    # Use pre-searched files if provided (e.g., from get_era5_input_files)
    if input_files is not None:
        files = input_files
        if isinstance(files, dict):
            total_files = sum(len(f) for f in files.values())
            logger.info(f"Using pre-searched files: {total_files} files across {len(files)} variables")
        else:
            logger.info(f"Using pre-searched files: {len(files)} files")
    else:
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
            concat_dim=concat_dim,
            combine_vars=combine_vars  # Merge multi-variable files (ERA5)
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
        
        # Pass config dictionary to remap_delaunay for flexible parameter handling
        ds_remap = remap_tools.remap_delaunay(ds, zoom, weights_file, config=config)
        step_time = time.time() - step_start
        logger.info(f"✅ Step 3 completed in {step_time:.1f} seconds")

        logger.info(f"Remapped dataset: {ds_remap.sizes}")
        logger.info(f"Variables: {list(ds_remap.data_vars)}")
        
        # Apply variable subsetting and renaming if requested
        if remap_variables:
            logger.info("🔄 Step 3b: Subsetting and renaming variables...")
            step_start = time.time()
            
            # Subset dataset to include variables in remap_variables + passthrough variables
            # This automatically includes coordinate variables (time, cell, lev, etc.)
            vars_to_keep = [var for var in remap_variables.keys() if var in ds_remap.data_vars]
            
            # Also keep passthrough variables (non-remapped variables like vertical coordinates)
            passthrough_variables = config.get('passthrough_variables', [])
            if passthrough_variables:
                passthrough_to_keep = [var for var in passthrough_variables if var in ds_remap.data_vars]
                vars_to_keep.extend(passthrough_to_keep)
                if passthrough_to_keep:
                    logger.info(f"   Including {len(passthrough_to_keep)} passthrough variable(s): {passthrough_to_keep}")
            
            if not vars_to_keep:
                logger.warning(f"   None of the variables in remap_variables found in dataset")
                logger.warning(f"   Requested: {list(remap_variables.keys())}")
                logger.warning(f"   Available: {list(ds_remap.data_vars)}")
            else:
                logger.info(f"   Subsetting to {len(vars_to_keep)} variable(s): {vars_to_keep}")
                ds_remap = ds_remap[vars_to_keep]
                
                # Rename the subsetted variables (but not passthrough variables)
                renamed_vars = []
                for old_name, new_name in remap_variables.items():
                    if old_name in ds_remap.data_vars:
                        ds_remap = ds_remap.rename({old_name: new_name})
                        renamed_vars.append(f"{old_name} → {new_name}")
                        logger.info(f"   Renamed: {old_name} → {new_name}")
                
                step_time = time.time() - step_start
                logger.info(f"✅ Step 3b completed in {step_time:.1f} seconds ({len(renamed_vars)} variable(s) processed)")
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
