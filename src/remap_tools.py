import numpy as np
import xarray as xr
import healpy as hp
import easygems.remap as egr
from pathlib import Path
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


def gen_weights(ds, order, weights_file=None, force_recompute=False):
    """
    Generate or load optimized remapping weights for lat/lon to HEALPix.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with lat/lon coordinates
    order : int
        HEALPix order (zoom level)
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
    
    logger.info(f"Computing new weights for HEALPix order {order}")
    nside = 2**order  # Using healpy convention: nside = 2^order
    npix = hp.nside2npix(nside)

    hp_lon, hp_lat = hp.pix2ang(
        nside=nside, ipix=np.arange(npix), lonlat=True, nest=True
    )
    
    # Create 2D meshgrid and flatten for regular lat/lon grid
    lon_2d, lat_2d = np.meshgrid(ds.lon.values, ds.lat.values)
    source_lon = lon_2d.flatten()
    source_lat = lat_2d.flatten()
    
    # Check if this is a global or regional grid
    lon_range = np.max(ds.lon.values) - np.min(ds.lon.values)
    is_global = lon_range >= 359  # Global grid spans ~360 degrees
    
    if is_global:
        # Handle longitude periodicity for global grids (like IMERG)
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
        'source_grid_type': 'regular_latlon',
        'source_grid_extent': 'global' if is_global else 'regional',
        'source_shape': f"{len(ds.lat)} x {len(ds.lon)}",
        'source_lon_range': f"{np.min(ds.lon):.3f} to {np.max(ds.lon):.3f}",
        'source_lat_range': f"{np.min(ds.lat):.3f} to {np.max(ds.lat):.3f}",
        'source_resolution': f"~{np.diff(ds.lon).mean():.4f}° x {np.diff(ds.lat).mean():.4f}°",
        'longitude_periodicity': 'enabled' if is_global else 'disabled',
        'creation_date': str(np.datetime64('now')),
        'description': f'Remapping weights from {"global" if is_global else "regional"} lat/lon grid to HEALPix'
    })
    
    # Save weights if requested
    if weights_file is not None:
        save_weights(weights, weights_file)
    
    return weights


def remap_delaunay(ds: xr.Dataset, order: int, weights_file=None, force_recompute=False) -> xr.Dataset:
    """
    Remap dataset to HEALPix using Delaunay triangulation with weight caching support.
    Expects lat/lon coordinates and remaps spatial variables.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with lat/lon coordinates
    order : int
        HEALPix order (zoom level)
    weights_file : str, optional
        Path to weights file for caching
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
    
    Returns:
    --------
    xr.Dataset : Remapped dataset with HEALPix grid
    """
    logger.info(f"Starting HEALPix remapping (zoom level {order})")
    logger.info(f"Dataset: {dict(ds.sizes)}, Variables: {list(ds.data_vars)}")
    
    weights = gen_weights(ds, order, weights_file, force_recompute)
    npix = len(weights.tgt_idx)
    
    logger.info(f"Weights loaded: {npix} target pixels, spatial size: {len(ds.lat) * len(ds.lon)}")
    
    # Remap variables with spatial dimensions
    remapped_vars = {}
    
    for var_name in ds.data_vars:
        data_array = ds[var_name]
        
        # Skip coordinate bounds variables
        if var_name.endswith('_bnds') or var_name.endswith('_bounds'):
            continue
            
        # Only process variables with spatial dimensions
        if not ('lat' in data_array.dims and 'lon' in data_array.dims):
            continue
            
        # Handle dimension order - transpose if needed
        if list(data_array.dims) == ['time', 'lon', 'lat']:
            data_array = data_array.transpose('time', 'lat', 'lon')
        
        logger.info(f"Processing variable '{var_name}': {data_array.shape} {data_array.dims}")
        
        # Stack spatial dimensions for remapping
        data_stacked = data_array.stack(spatial=('lat', 'lon'))
        
        # Ensure spatial dimension is in a single chunk for remapping
        data_stacked = data_stacked.chunk({'spatial': -1})
        
        # Validate spatial dimensions
        expected_spatial_size = len(ds.lat) * len(ds.lon)
        actual_spatial_size = data_stacked.sizes['spatial']
        if expected_spatial_size != actual_spatial_size:
            raise ValueError(f"Spatial size mismatch: expected {expected_spatial_size}, got {actual_spatial_size}")
        
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
            data_stacked,
            input_core_dims=[['spatial']],
            output_core_dims=[['cell']],
            dask='parallelized',  # Use parallelized to handle complex operations
            output_dtypes=[data_stacked.dtype],
            dask_gufunc_kwargs={'output_sizes': {'cell': npix}},
        )
        
        logger.info(f"Remapped data shape: {remapped_data.shape}")
        
        # Create new DataArray with proper coordinates
        remapped_vars[var_name] = xr.DataArray(
            remapped_data,
            dims=['time', 'cell'],
            coords={
                'time': data_array.coords['time'],
                'cell': np.arange(npix)
            },
            attrs=data_array.attrs
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
        'original_grid': 'regular_lat_lon',
        'remapping_method': 'delaunay_triangulation'
    })
    
    return ds_remap
