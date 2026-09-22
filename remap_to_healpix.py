#!/usr/bin/env python3
"""
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


def process_to_healpix_zarr(
    start_date,
    end_date,
    zoom,
    output_zarr,
    weights_file=None,
    overwrite=False,
    time_average=None,
    preprocessing_func=None,
    preprocessing_kwargs=None,
    config=None,
    dataset=None,
):
    """
    Generalized pipeline for remapping datasets to HEALPix Zarr.
    Supports both:
        1. file-based workflows
        2. preloaded xarray datasets (e.g. AWS Zarr / CCIC)
    """

    import time

    if config is None:
        raise ValueError("config dictionary is required")

    # =========================================================
    # CONFIG
    # =========================================================

    input_base_dir = config.get("input_base_dir", None)

    time_chunk_size = config.get("time_chunk_size", 48)

    spatial_chunks = config.get("spatial_dimensions", None)

    concat_dim = config.get("concat_dim", "time")

    force_recompute = config.get("force_recompute", False)

    grid_type = config.get("grid_type", "auto")

    convert_time = config.get("convert_time", False)

    dask_config = config.get("dask", None)

    date_pattern = config.get(
        "date_pattern",
        r"\.(\d{8})-"
    )

    date_format = config.get(
        "date_format",
        "%Y%m%d"
    )

    use_year_subdirs = config.get(
        "use_year_subdirs",
        True
    )

    file_glob = config.get(
        "file_glob",
        "*.nc*"
    )

    skip_variables = config.get(
        "skip_variables",
        None
    )

    required_dimensions = config.get(
        "required_dimensions",
        None
    )

    remap_variables = config.get(
        "remap_variables",
        None
    )

    input_files = config.get(
        "input_files",
        None
    )

    combine_vars = config.get(
        "combine_vars",
        False
    )

    # =========================================================
    # LOGGING
    # =========================================================

    logger.info("=" * 70)
    logger.info("Configuration Summary")
    logger.info("=" * 70)

    if dataset is not None:
        logger.info("Input source: preloaded xarray dataset")
    else:
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

    logger.info("=" * 70)

    # =========================================================
    # LOAD DATASET
    # =========================================================

    files = None

    if dataset is not None:

        logger.info("📦 Using preloaded dataset")

        ds = dataset

    else:

        logger.info("📁 Using file-based input pipeline")

        # ---------------------------------------------
        # use pre-searched files if provided
        # ---------------------------------------------

        if input_files is not None:

            files = input_files

            if isinstance(files, dict):

                total_files = sum(
                    len(v) for v in files.values()
                )

                logger.info(
                    f"Using pre-searched files: "
                    f"{total_files} files across "
                    f"{len(files)} variables"
                )

            else:

                logger.info(
                    f"Using pre-searched files: "
                    f"{len(files)} files"
                )

        else:

            files = utilities.get_input_files(
                start_date,
                end_date,
                input_base_dir,
                date_pattern=date_pattern,
                date_format=date_format,
                use_year_subdirs=use_year_subdirs,
                file_glob=file_glob,
            )

            if not files:
                raise ValueError(
                    "No files found for specified period"
                )

        ds = utilities.read_concat_files(
            files,
            time_chunk_size=time_chunk_size,
            spatial_dims=spatial_chunks,
            concat_dim=concat_dim,
            combine_vars=combine_vars,
        )

    # =========================================================
    # SPATIAL DIMENSION DETECTION
    # =========================================================

    if spatial_chunks is None:

        logger.info(
            "🔍 Spatial dimensions not specified"
        )

        if dataset is not None:

            logger.info(
                "Auto-detecting from dataset..."
            )

            spatial_chunks = {
                dim: -1
                for dim in ds.dims
                if dim != concat_dim
            }

        else:

            logger.info(
                "Auto-detecting from files..."
            )

            spatial_chunks = (
                utilities.detect_spatial_dimensions(
                    files,
                    time_dim=concat_dim,
                )
            )

    logger.info(
        f"Using spatial chunks: {spatial_chunks}"
    )

    # =========================================================
    # DASK
    # =========================================================

    if dask_config:

        client = utilities.setup_dask_client(
            n_workers=dask_config.get(
                "n_workers"
            ),
            threads_per_worker=dask_config.get(
                "threads_per_worker"
            ),
            memory_limit=dask_config.get(
                "memory_limit"
            ),
            advanced_config=dask_config.get(
                "worker_options",
                {},
            ),
        )

    else:

        client = utilities.setup_dask_client()

    logger.info(
        f"Dask workers: "
        f"{len(client.scheduler_info()['workers'])}"
    )

    # =========================================================
    # PROCESSING TIMER
    # =========================================================

    overall_start_time = time.time()

    try:

        # =====================================================
        # PREPROCESSING
        # =====================================================

        if preprocessing_func is not None:

            logger.info(
                "🔄 Applying preprocessing..."
            )

            step_start = time.time()

            funcs = (
                preprocessing_func
                if isinstance(preprocessing_func, list)
                else [preprocessing_func]
            )

            kwargs_list = (
                preprocessing_kwargs
                if isinstance(preprocessing_kwargs, list)
                else [preprocessing_kwargs or {}]
            )

            if len(kwargs_list) == 1 and len(funcs) > 1:
                kwargs_list *= len(funcs)

            for i, (func, kwargs) in enumerate(
                zip(funcs, kwargs_list)
            ):

                func_name = getattr(
                    func,
                    "__name__",
                    str(func),
                )

                logger.info(
                    f"Applying preprocessing "
                    f"{i+1}/{len(funcs)}: "
                    f"{func_name}"
                )

                ds = func(ds, **kwargs)

            logger.info(
                f"✅ preprocessing completed "
                f"in {(time.time()-step_start):.1f}s"
            )

        # =====================================================
        # TEMPORAL AVERAGING
        # =====================================================

        logger.info(
            "🔄 Applying temporal averaging..."
        )

        step_start = time.time()

        ds = utilities.temporal_average(
            ds,
            time_average,
            convert_time,
        )

        logger.info(
            f"✅ temporal averaging completed "
            f"in {(time.time()-step_start):.1f}s"
        )

        # =====================================================
        # REMAP
        # =====================================================

        logger.info(
            f"🔄 Remapping to HEALPix zoom {zoom}"
        )

        step_start = time.time()

        ds_remap = remap_tools.remap_delaunay(
            ds,
            zoom,
            weights_file,
            config=config,
        )

        logger.info(
            f"✅ remapping completed "
            f"in {(time.time()-step_start):.1f}s"
        )

        logger.info(
            f"Remapped dataset dims: "
            f"{ds_remap.sizes}"
        )

        logger.info(
            f"Variables: "
            f"{list(ds_remap.data_vars)}"
        )

        # =====================================================
        # VARIABLE RENAME
        # =====================================================

        if remap_variables:

            logger.info(
                "🔄 Applying variable renaming..."
            )

            vars_to_keep = [
                var
                for var in remap_variables.keys()
                if var in ds_remap.data_vars
            ]

            passthrough_variables = config.get(
                "passthrough_variables",
                [],
            )

            vars_to_keep.extend(
                [
                    var
                    for var in passthrough_variables
                    if var in ds_remap.data_vars
                ]
            )

            ds_remap = ds_remap[vars_to_keep]

            for old_name, new_name in (
                remap_variables.items()
            ):

                if old_name in ds_remap:

                    ds_remap = ds_remap.rename(
                        {old_name: new_name}
                    )

                    logger.info(
                        f"Renamed "
                        f"{old_name} -> {new_name}"
                    )

        # =====================================================
        # WRITE ZARR
        # =====================================================

        logger.info("🔄 Writing Zarr...")

        step_start = time.time()

        ds_remap_chunked, zarr_time = (
            zarr_tools.write_zarr_with_monitoring(
                ds_remap,
                output_zarr,
                time_chunk_size,
                zoom,
                overwrite,
            )
        )

        logger.info(
            f"✅ Zarr write completed "
            f"in {zarr_time/60:.1f} min"
        )

        # =====================================================
        # SUMMARY
        # =====================================================

        total_time = time.time() - overall_start_time

        logger.info("=" * 60)
        logger.info("📈 PROCESSING SUMMARY")
        logger.info("=" * 60)

        if dataset is not None:

            logger.info(
                "Input source: "
                "preloaded xarray dataset"
            )

        else:

            logger.info(
                f"Input files: {len(files)}"
            )

            logger.info(
                f"Processing rate: "
                f"{len(files)/(total_time/60):.1f} "
                f"files/minute"
            )

        logger.info(
            f"Total runtime: "
            f"{total_time/60:.1f} minutes"
        )

        logger.info(
            f"Output size: "
            f"{ds_remap_chunked.nbytes / 1024**3:.2f} GB"
        )

        logger.info("=" * 60)

        logger.info(
            f"Successfully created:\n{output_zarr}"
        )

        return ds_remap_chunked

    finally:

        logger.info(
            "Shutting down Dask cluster..."
        )

        try:
            client.shutdown()
        except Exception:
            pass

        try:
            client.close()
        except Exception:
            pass




# Backwards compatibility: maintain old function name as alias
def process_imerg_to_zarr(*args, **kwargs):
    """
    Backwards compatibility alias for process_to_healpix_zarr().
    
    This function name is deprecated but maintained for backwards compatibility.
    Please use process_to_healpix_zarr() for new code.
    """
    return process_to_healpix_zarr(*args, **kwargs)
