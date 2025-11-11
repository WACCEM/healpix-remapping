import numpy as np
import xarray as xr
import healpy as hp
import easygems.remap as egr
from pathlib import Path
import fnmatch
import logging

# Configure logging to ensure output appears in terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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


def gen_weights(ds, order, weights_file=None, force_recompute=False, grid_type='auto', 
                lon_name='lon', lat_name='lat'):
    """
    Generate or load optimized remapping weights for spatial coordinates to HEALPix.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with spatial coordinates
    order : int
        HEALPix order (zoom level)
    weights_file : str, optional
        Path to weights file for caching
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
    grid_type : str, default='auto'
        Type of input grid:
        - 'auto': Automatically detect from coordinate dimensions
        - 'latlon_1d': Regular lat/lon with 1D coordinates (requires meshgrid)
        - 'latlon_2d': Curvilinear grid with 2D lat/lon arrays
        - 'unstructured': Unstructured grid with 1D coordinate per cell (e.g., E3SM, SCREAM)
    lon_name : str, default='lon'
        Name of longitude coordinate variable
    lat_name : str, default='lat'
        Name of latitude coordinate variable
        
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
    
    logger.info(f"Computing new weights for HEALPix order {order}")
    nside = 2**order  # Using healpy convention: nside = 2^order
    npix = hp.nside2npix(nside)

    hp_lon, hp_lat = hp.pix2ang(
        nside=nside, ipix=np.arange(npix), lonlat=True, nest=True
    )
    
    # Get lon/lat coordinates using configurable names
    lon_coord = ds[lon_name]
    lat_coord = ds[lat_name]
    
    # Auto-detect grid type if not specified
    if grid_type == 'auto':
        logger.info("Auto-detecting grid type from coordinate dimensions...")
        if lon_coord.ndim == 1 and lat_coord.ndim == 1:
            # Check for unstructured grid indicators
            if 'ncol' in ds.dims or 'cell' in ds.dims:
                grid_type = 'unstructured'
                logger.info("  Detected: unstructured grid (ncol/cell dimension)")
            else:
                grid_type = 'latlon_1d'
                logger.info("  Detected: regular 1D lat/lon grid")
        elif lon_coord.ndim == 2 and lat_coord.ndim == 2:
            grid_type = 'latlon_2d'
            logger.info("  Detected: 2D lat/lon grid (curvilinear)")
        else:
            raise ValueError(f"Cannot auto-detect grid type: {lon_name}.ndim={lon_coord.ndim}, {lat_name}.ndim={lat_coord.ndim}")
    else:
        logger.info(f"Using specified grid type: {grid_type}")
    
    # Handle different grid types
    if grid_type == 'latlon_1d':
        # Regular lat/lon grid with 1D coordinates - create meshgrid
        logger.info("Processing regular 1D lat/lon grid (creating meshgrid)...")
        lon_2d, lat_2d = np.meshgrid(lon_coord.values, lat_coord.values)
        source_lon = lon_2d.flatten()
        source_lat = lat_2d.flatten()
        source_shape = f"{len(lat_coord)} x {len(lon_coord)}"
        
    elif grid_type == 'latlon_2d':
        # 2D curvilinear coordinates - just flatten
        logger.info("Processing 2D lat/lon grid (curvilinear, flattening)...")
        source_lon = lon_coord.values.flatten()
        source_lat = lat_coord.values.flatten()
        source_shape = f"{lon_coord.shape[0]} x {lon_coord.shape[1]}"
        
    elif grid_type == 'unstructured':
        # Unstructured grid - coordinates already 1D per cell
        logger.info("Processing unstructured grid (1D coordinates per cell)...")
        source_lon = lon_coord.values
        source_lat = lat_coord.values
        
        # Validate that we have matching dimensions
        if len(source_lon) != len(source_lat):
            raise ValueError(f"Unstructured grid: lon and lat must have same length (got {len(source_lon)} vs {len(source_lat)})")
        
        # Determine the cell dimension name
        if 'ncol' in ds.dims:
            source_shape = f"{len(source_lon)} (ncol)"
        elif 'cell' in ds.dims:
            source_shape = f"{len(source_lon)} (cell)"
        else:
            source_shape = f"{len(source_lon)} cells"
    else:
        raise ValueError(f"Unknown grid_type: {grid_type}. Must be 'auto', 'latlon_1d', 'latlon_2d', or 'unstructured'")
    
    logger.info(f"Source grid size: {len(source_lon)} points")
    
    # Check if this is a global or regional grid
    lon_range = np.max(source_lon) - np.min(source_lon)
    is_global = lon_range >= 359  # Global grid spans ~360 degrees
    
    if is_global:
        # Handle longitude periodicity for global grids
        logger.info("Global grid detected - handling longitude periodicity")
        lon_extended = np.hstack([source_lon - 360, source_lon, source_lon + 360])
        lat_extended = np.tile(source_lat, 3)
        
        # Compute weights using extended grid
        weights = egr.compute_weights_delaunay(
            points=(lon_extended, lat_extended), 
            xi=(hp_lon, hp_lat)
        )

        # Remap the source indices back to their valid range
        original_size = len(source_lon)
        weights = weights.assign(src_idx=weights.src_idx % original_size)
        
    else:
        # Regional grid - no periodicity handling needed
        logger.info(f"Regional grid detected (lon range: {lon_range:.1f}°) - no periodicity handling")
        weights = egr.compute_weights_delaunay(
            points=(source_lon, source_lat), 
            xi=(hp_lon, hp_lat)
        )
    
    # Add metadata
    weights.attrs.update({
        'healpix_order': order,
        'healpix_nside': nside,
        'healpix_npix': npix,
        'source_grid_type': grid_type,
        'source_grid_extent': 'global' if is_global else 'regional',
        'source_shape': source_shape,
        'source_lon_range': f"{np.min(source_lon):.3f} to {np.max(source_lon):.3f}",
        'source_lat_range': f"{np.min(source_lat):.3f} to {np.max(source_lat):.3f}",
        'longitude_periodicity': 'enabled' if is_global else 'disabled',
        'creation_date': str(np.datetime64('now')),
        'description': f'Remapping weights from {grid_type} {"global" if is_global else "regional"} grid to HEALPix'
    })
    
    # Add resolution info for regular grids
    if grid_type == 'latlon_1d':
        weights.attrs['source_resolution'] = f"~{np.diff(ds.lon).mean():.4f}° x {np.diff(ds.lat).mean():.4f}°"
    
    # Save weights if requested
    if weights_file is not None:
        save_weights(weights, weights_file)
    
    return weights


def remap_delaunay(ds: xr.Dataset, order: int, weights_file=None, config=None) -> xr.Dataset:
    """
    Remap dataset to HEALPix using Delaunay triangulation with weight caching support.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with spatial coordinates
    order : int
        HEALPix order (zoom level)
    weights_file : str, optional
        Path to weights file for caching
    config : dict, optional
        Configuration dictionary containing remapping parameters:
        - force_recompute: Force recompute weights (default: False)
        - grid_type: Grid type ('auto', 'latlon_1d', 'latlon_2d', 'unstructured')
        - skip_variables: List of variable patterns to skip
        - required_dimensions: List of required dimension combinations
        - x_dimname: Name of x spatial dimension (e.g., 'lon', 'west_east')
        - y_dimname: Name of y spatial dimension (e.g., 'lat', 'south_north')
        - spatial_dimensions: Dict of spatial dimension names with chunk sizes
        - lon_name: Name of longitude coordinate variable (default: 'lon')
        - lat_name: Name of latitude coordinate variable (default: 'lat')
        
        Note: For 2D grids, MUST specify both x_dimname and y_dimname to avoid ambiguity.
              For 1D unstructured grids, only x_dimname is used (y_dimname will be None).
    
    Returns:
    --------
    xr.Dataset : Remapped dataset with HEALPix grid
    
    Examples:
    ---------
    # Standard lat/lon grid (uses defaults)
    config = {'grid_type': 'latlon_1d'}
    ds_remap = remap_delaunay(ds, zoom=9, weights_file='weights.nc', config=config)
    
    # Standard lat/lon grid (explicit dimension names)
    config = {
        'grid_type': 'latlon_1d',
        'x_dimname': 'lon',
        'y_dimname': 'lat',
        'spatial_dimensions': {'lon': -1, 'lat': -1}
    }
    ds_remap = remap_delaunay(ds, zoom=9, weights_file='weights.nc', config=config)
    
    # WRF grid with custom dimension and coordinate names
    config = {
        'grid_type': 'latlon_2d',
        'x_dimname': 'west_east',
        'y_dimname': 'south_north',
        'lon_name': 'XLONG',
        'lat_name': 'XLAT',
        'spatial_dimensions': {'west_east': -1, 'south_north': -1}
    }
    ds_remap = remap_delaunay(ds, zoom=9, weights_file='weights.nc', config=config)
    
    # Unstructured grid (E3SM/SCREAM) - only x_dimname needed
    config = {
        'grid_type': 'unstructured',
        'x_dimname': 'ncol',
        'spatial_dimensions': {'ncol': -1},
        'skip_variables': ['*_bounds', 'time_bnds'],
        'required_dimensions': [['time', 'ncol']]
    }
    ds_remap = remap_delaunay(ds, zoom=9, weights_file='weights.nc', config=config)
    """
    
    # Extract parameters from config with defaults
    if config is None:
        config = {}
    
    force_recompute = config.get('force_recompute', False)
    grid_type = config.get('grid_type', 'auto')
    skip_variables = config.get('skip_variables', None)
    required_dimensions = config.get('required_dimensions', None)
    spatial_dimensions = config.get('spatial_dimensions', None)
    
    # Get coordinate names with defaults
    lon_name = config.get('lon_name', 'lon')
    lat_name = config.get('lat_name', 'lat')
    
    # Get explicit dimension names from config (preferred method)
    x_dimname = config.get('x_dimname', None)
    y_dimname = config.get('y_dimname', None)
    
    # Determine spatial dimension names from config
    if x_dimname is not None and y_dimname is not None:
        # User explicitly specified x and y dimension names (preferred)
        logger.info(f"Using explicit dimension names from config: x='{x_dimname}', y='{y_dimname}'")
        
        # Validate that these dimensions exist in spatial_dimensions if specified
        if spatial_dimensions:
            if x_dimname not in spatial_dimensions or y_dimname not in spatial_dimensions:
                raise ValueError(f"Dimension names x='{x_dimname}', y='{y_dimname}' must be in spatial_dimensions: {list(spatial_dimensions.keys())}")
    
    elif spatial_dimensions:
        # User specified spatial dimensions but not explicit x/y names
        # This is ambiguous and error-prone - raise informative error
        spatial_dim_names = list(spatial_dimensions.keys())
        
        if len(spatial_dim_names) == 1:
            # Unstructured grid - single spatial dimension
            x_dimname = spatial_dim_names[0]
            y_dimname = None
            logger.info(f"Single spatial dimension detected (unstructured grid): '{x_dimname}'")
        elif len(spatial_dim_names) == 2:
            # Ambiguous! Require explicit x_dimname and y_dimname
            raise ValueError(
                f"Ambiguous spatial dimension order: {spatial_dim_names}\n"
                f"Please explicitly specify x_dimname and y_dimname in config.\n"
                f"Example:\n"
                f"  x_dimname: '{spatial_dim_names[0]}'\n"
                f"  y_dimname: '{spatial_dim_names[1]}'\n"
                f"  spatial_dimensions:\n"
                f"    {spatial_dim_names[0]}: -1\n"
                f"    {spatial_dim_names[1]}: -1"
            )
        else:
            raise ValueError(f"spatial_dimensions must have 1 or 2 dimensions, got {len(spatial_dim_names)}")
    else:
        # No spatial_dimensions specified - use standard defaults
        x_dimname = 'lon'
        y_dimname = 'lat'
        logger.info(f"No spatial_dimensions or explicit dimension names specified, using defaults: x='lon', y='lat'")
    
    logger.info(f"Starting HEALPix remapping (zoom level {order})")
    logger.info(f"Dataset: {dict(ds.sizes)}, Variables: {list(ds.data_vars)}")
    logger.info(f"Coordinate names: lon='{lon_name}', lat='{lat_name}'")
    logger.info(f"Dimension names: x='{x_dimname}', y='{y_dimname}'")
    
    # Helper function to check if variable matches skip pattern
    def should_skip_variable(var_name, skip_patterns):
        """Check if variable name matches any skip pattern (supports wildcards)."""
        if skip_patterns is None:
            return False
        for pattern in skip_patterns:
            if fnmatch.fnmatch(var_name, pattern):
                return True
        return False
    
    # Helper function to check if variable has required dimensions
    def has_required_dimensions(var_dims, required_dim_sets):
        """Check if variable dimensions match any required dimension set."""
        if required_dim_sets is None:
            return True  # No requirement, all pass
        for required_dims in required_dim_sets:
            if all(dim in var_dims for dim in required_dims):
                return True
        return False
    
    weights = gen_weights(ds, order, weights_file, force_recompute, grid_type, 
                         lon_name=lon_name, lat_name=lat_name)
    npix = len(weights.tgt_idx)
    
    # Calculate spatial size based on grid type and dimension names
    if y_dimname is None:
        # Unstructured grid - single spatial dimension
        spatial_size = ds.sizes[x_dimname]
    else:
        # Structured grid - product of two dimensions
        lat_coord = ds[lat_name] if lat_name in ds else ds[y_dimname]
        lon_coord = ds[lon_name] if lon_name in ds else ds[x_dimname]
        spatial_size = len(lat_coord) * len(lon_coord)
    
    logger.info(f"Weights loaded: {npix} target pixels, source spatial size: {spatial_size}")
    
    # Remap variables with spatial dimensions
    remapped_vars = {}
    
    # Determine spatial dimension names based on grid type
    detected_grid_type = weights.attrs.get('source_grid_type', 'latlon_1d')
    
    for var_name in ds.data_vars:
        data_array = ds[var_name]
        
        # Skip variables matching skip patterns
        if should_skip_variable(var_name, skip_variables):
            logger.info(f"Skipping variable '{var_name}' (matches skip pattern)")
            continue
        
        # Check if variable has required dimensions
        if not has_required_dimensions(data_array.dims, required_dimensions):
            logger.info(f"Skipping variable '{var_name}' (dimensions {list(data_array.dims)} don't match required)")
            continue
        
        # Skip coordinate bounds variables
        if var_name.endswith('_bnds') or var_name.endswith('_bounds'):
            continue
        
        # Determine if this variable has spatial dimensions based on grid type
        has_spatial = False
        spatial_dims = []
        
        if detected_grid_type == 'unstructured':
            # For unstructured grids, look for the configured spatial dimension
            if x_dimname in data_array.dims:
                has_spatial = True
                spatial_dims = [x_dimname]
        else:
            # For regular/curvilinear grids, look for both x and y dimensions
            if x_dimname in data_array.dims and y_dimname in data_array.dims:
                has_spatial = True
                spatial_dims = [y_dimname, x_dimname]  # [lat/south_north, lon/west_east]
        
        # Only process variables with spatial dimensions
        if not has_spatial:
            continue
        
        logger.info(f"Processing variable '{var_name}': {data_array.shape} {data_array.dims}")
        
        # Handle different grid structures
        if detected_grid_type == 'unstructured':
            # Unstructured grid - spatial dimension is already 1D
            spatial_dim_name = spatial_dims[0]
            
            # Ensure spatial dimension is in a single chunk for remapping
            data_array = data_array.chunk({spatial_dim_name: -1})
            
            # Validate spatial dimensions
            expected_spatial_size = ds.sizes[spatial_dim_name]
            actual_spatial_size = data_array.sizes[spatial_dim_name]
            if expected_spatial_size != actual_spatial_size:
                raise ValueError(f"Spatial size mismatch for {var_name}: expected {expected_spatial_size}, got {actual_spatial_size}")
            
        else:
            # Regular or curvilinear grid - need to stack spatial dimensions
            # Handle dimension order - transpose if needed to have (time, y, x) order
            expected_order = ['time', y_dimname, x_dimname]
            if list(data_array.dims) == ['time', x_dimname, y_dimname]:
                data_array = data_array.transpose('time', y_dimname, x_dimname)
                logger.info(f"  Transposed dimensions from ['time', '{x_dimname}', '{y_dimname}'] to {expected_order}")
            
            # Stack spatial dimensions for remapping
            data_array = data_array.stack(spatial=(y_dimname, x_dimname))
            spatial_dim_name = 'spatial'
            
            # Ensure spatial dimension is in a single chunk for remapping
            data_array = data_array.chunk({'spatial': -1})
            
            # Validate spatial dimensions
            lat_coord = ds[lat_name] if lat_name in ds else ds[y_dimname]
            lon_coord = ds[lon_name] if lon_name in ds else ds[x_dimname]
            expected_spatial_size = len(lat_coord) * len(lon_coord)
            actual_spatial_size = data_array.sizes['spatial']
            if expected_spatial_size != actual_spatial_size:
                raise ValueError(f"Spatial size mismatch for {var_name}: expected {expected_spatial_size}, got {actual_spatial_size}")
        
        # Apply remapping with Dask for parallel processing
        logger.info("Applying remapping with Dask parallel processing...")
        
        # Compute weights to numpy arrays for use in parallel function
        weights_computed = {var: weights[var].values for var in weights.data_vars}
        
        # Apply weights using xr.apply_ufunc with proper Dask handling
        def apply_weights_with_time(data_chunk):
            """Apply weights to a data chunk, handling time dimension properly."""
            # Ensure data is numpy array
            if hasattr(data_chunk, 'values'):
                data_chunk = data_chunk.values
            
            if data_chunk.ndim == 2:  # (time, spatial)
                # Process each time step
                remapped_list = []
                for t in range(data_chunk.shape[0]):
                    time_slice = data_chunk[t, :]  # Shape: (spatial,)
                    remapped_time = egr.apply_weights(time_slice, **weights_computed)
                    remapped_list.append(remapped_time)
                return np.stack(remapped_list, axis=0)  # Shape: (time, npix)
            else:  # 1D spatial data
                return egr.apply_weights(data_chunk, **weights_computed)
        
        # Use xr.apply_ufunc for Dask-compatible processing
        remapped_data = xr.apply_ufunc(
            apply_weights_with_time,
            data_array,
            input_core_dims=[[spatial_dim_name]],
            output_core_dims=[['cell']],
            dask='parallelized',  # Use parallelized to handle complex operations
            output_dtypes=[data_array.dtype],
            dask_gufunc_kwargs={'output_sizes': {'cell': npix}},
        )
        
        logger.info(f"Remapped data shape: {remapped_data.shape}")
        
        # Create new DataArray with proper coordinates
        # Get the original data_array reference from ds (not the modified version)
        orig_data_array = ds[var_name]
        remapped_vars[var_name] = xr.DataArray(
            remapped_data,
            dims=['time', 'cell'],
            coords={
                'time': orig_data_array.coords['time'],
                'cell': np.arange(npix)
            },
            attrs=orig_data_array.attrs
        )
    
    # Create remapped dataset
    ds_remap = xr.Dataset(remapped_vars)
    
    # Add CRS coordinate (scalar)
    nside = 2**order
    crs_var = xr.DataArray(
        data=0,
        dims=[],
        attrs={
            'grid_mapping_name': 'healpix',
            'healpix_nside': nside,
            'healpix_order': 'nest'
        }
    )
    ds_remap = ds_remap.assign_coords(crs=crs_var)
    
    # Update dataset attributes
    ds_remap.attrs.update({
        'healpix_order': order,
        'healpix_nside': nside,
        'healpix_npix': npix,
        'healpix_nest': True,
        'grid_mapping': 'crs',
        'original_grid': detected_grid_type,
        'remapping_method': 'delaunay_triangulation'
    })
    
    return ds_remap
