#!/usr/bin/env python3
"""
Simple script to remap your specific dataset to HEALPix grid.
Usage: python remap_merg_to_healpix.py <input_file> [output_file]
"""

import sys
import numpy as np
import xarray as xr
import healpix as hp
import easygems.remap as egr
from pathlib import Path


def save_weights(weights, weights_file):
    """Save remapping weights to NetCDF file."""
    weights_path = Path(weights_file)
    weights_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Saving weights to {weights_path}")
    
    # Use compression for efficient storage
    encoding = {}
    for var in weights.data_vars:
        if weights[var].dtype.kind in ['i', 'u']:  # Integer variables
            encoding[var] = {'zlib': True, 'complevel': 4, 'dtype': 'int32'}
        elif weights[var].dtype.kind == 'f':  # Float variables
            encoding[var] = {'zlib': True, 'complevel': 4, 'dtype': 'float32'}
    
    weights.to_netcdf(weights_path, encoding=encoding)
    print(f"Weights saved successfully ({weights_path.stat().st_size / 1024**2:.1f} MB)")


def load_weights(weights_file):
    """Load remapping weights from NetCDF file."""
    weights_path = Path(weights_file)
    if not weights_path.exists():
        raise FileNotFoundError(f"Weights file not found: {weights_path}")
    
    print(f"Loading weights from {weights_path}")
    weights = xr.open_dataset(weights_path)
    
    # Print some info about the loaded weights
    if 'healpix_order' in weights.attrs:
        print(f"  HEALPix order: {weights.attrs['healpix_order']}")
    if 'source_shape' in weights.attrs:
        print(f"  Source grid: {weights.attrs['source_shape']}")
    if 'creation_date' in weights.attrs:
        print(f"  Created: {weights.attrs['creation_date']}")
    
    return weights


def gen_weights_latlon(lon, lat, zoom, weights_file=None, force_recompute=False):
    """
    Generate remapping weights from regular lat/lon grid to HEALPix grid.
    
    Parameters:
    -----------
    lon : array-like
        1D array of longitude values in degrees
    lat : array-like  
        1D array of latitude values in degrees
    zoom : int
        HEALPix zoom level (order)
    weights_file : str or Path, optional
        Path to save/load weights NetCDF file
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
        
    Returns:
    --------
    weights : Dataset
        Remapping weights for use with easygems.remap.apply_weights
    """
    
    # Check if we should load existing weights
    if weights_file is not None and not force_recompute:
        weights_path = Path(weights_file)
        if weights_path.exists():
            try:
                return load_weights(weights_path)
            except Exception as e:
                print(f"Error loading weights: {e}, recomputing...")
    
    nside = hp.order2nside(zoom)
    npix = hp.nside2npix(nside)
    
    # Get HEALPix pixel coordinates
    hp_lon, hp_lat = hp.pix2ang(
        nside=nside, ipix=np.arange(npix), lonlat=True, nest=True
    )
    
    # Create 2D meshgrid from 1D lat/lon arrays
    lon_2d, lat_2d = np.meshgrid(lon, lat)
    
    # Flatten to 1D for weight computation
    source_lon = lon_2d.flatten()
    source_lat = lat_2d.flatten()
    
    # For global grids, handle periodicity
    if np.max(lon) - np.min(lon) >= 359:  # Global grid
        print("Handling longitude periodicity for global grid")
        
        # Extend grid periodically in longitude
        lon_extended = np.hstack([source_lon - 360, source_lon, source_lon + 360])
        lat_extended = np.tile(source_lat, 3)
        
        # Compute weights using extended grid
        weights = egr.compute_weights_delaunay(
            points=(lon_extended, lat_extended), 
            xi=(hp_lon, hp_lat)
        )
        
        # Remap source indices back to original grid size
        original_size = len(source_lon)
        weights = weights.assign(src_idx=weights.src_idx % original_size)
        
    else:
        # Regional grid - no periodicity handling
        print("Regional grid detected, no periodicity handling")
        weights = egr.compute_weights_delaunay(
            points=(source_lon, source_lat), 
            xi=(hp_lon, hp_lat)
        )
    
    # Add metadata about the grid configuration
    weights.attrs.update({
        'healpix_order': zoom,
        'healpix_nside': nside,
        'healpix_npix': npix,
        'source_grid_type': 'regular_latlon',
        'source_shape': f"{len(lat)} x {len(lon)}",
        'source_lon_range': f"{np.min(lon):.3f} to {np.max(lon):.3f}",
        'source_lat_range': f"{np.min(lat):.3f} to {np.max(lat):.3f}",
        'source_resolution': f"~{np.diff(lon).mean():.4f}° x {np.diff(lat).mean():.4f}°",
        'creation_date': str(np.datetime64('now')),
        'description': 'Remapping weights from regular lat/lon grid to HEALPix'
    })
    
    # Save weights if requested
    if weights_file is not None:
        save_weights(weights, weights_file)
    
    return weights


def remap_latlon_to_healpix(ds, zoom, weights_file=None, force_recompute=False):
    """
    Remap a dataset from regular lat/lon grid to HEALPix grid.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with lat/lon coordinates
    zoom : int
        HEALPix zoom level (order)
    weights_file : str or Path, optional
        Path to save/load weights NetCDF file
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
        
    Returns:
    --------
    ds_remap : xr.Dataset
        Dataset remapped to HEALPix grid with 'cell' dimension
    """
    
    # Generate remapping weights
    print(f"Generating weights for HEALPix zoom {zoom}")
    weights = gen_weights_latlon(ds.lon.values, ds.lat.values, zoom, weights_file, force_recompute)
    
    # Get HEALPix parameters
    nside = hp.order2nside(zoom)
    npix = len(weights.tgt_idx)
    print(f"Remapping to {npix} HEALPix pixels (nside={nside})")
    
    # Custom remapping function that properly handles the data structure
    def remap_variable(data_array_xr, data_values, src_idx, weights_vals, valid):
        """Apply weights to a single variable with proper data handling."""
        # Ensure data is loaded and not a dask array for indexing
        if hasattr(data_values, 'compute'):
            data_values = data_values.compute()
        
        # Check if we need to transpose based on xarray dimension order
        original_shape = data_values.shape
        dims = data_array_xr.dims
        
        if len(original_shape) == 3:  # time, lat, lon or time, lon, lat
            # Handle both (time, lat, lon) and (time, lon, lat) cases
            if 'lat' in dims and 'lon' in dims:
                # Check dimension order and transpose if needed
                if list(dims) == ['time', 'lon', 'lat']:
                    # Transpose from (time, lon, lat) to (time, lat, lon)
                    data_values = data_values.transpose(0, 2, 1)
                    original_shape = data_values.shape
                
            data_flat = data_values.reshape(original_shape[0], -1)  # time, lat*lon
            result = np.zeros((original_shape[0], len(valid)))
            
            for t in range(original_shape[0]):
                result[t] = np.where(valid, (data_flat[t][src_idx] * weights_vals).sum(axis=-1), np.nan)
                
        elif len(original_shape) == 2:  # lat, lon
            # For 2D variables, check if they have spatial dimensions that match the grid
            expected_size = len(ds.lat) * len(ds.lon)
            actual_size = np.prod(original_shape)
            
            if actual_size != expected_size:
                print(f"Skipping variable with incompatible dimensions: {original_shape} (expected {len(ds.lat)}x{len(ds.lon)})")
                return None
                
            data_flat = data_values.flatten()  # lat*lon
            result = np.where(valid, (data_flat[src_idx] * weights_vals).sum(axis=-1), np.nan)
        else:
            raise ValueError(f"Unsupported data shape: {original_shape}")
            
        return result
    
    # Filter data variables to only include those with spatial dimensions
    def should_remap_variable(var_name, data_array):
        """Determine if a variable should be remapped based on its dimensions."""
        # Skip coordinate bounds variables
        if var_name.endswith('_bnds') or var_name.endswith('_bounds'):
            return False
            
        # Only remap variables that have both lat and lon dimensions
        # or have spatial dimensions matching the grid
        dims = data_array.dims
        
        if 'lat' in dims and 'lon' in dims:
            return True
        
        # For variables without explicit lat/lon dims, check if shape matches spatial grid
        if len(data_array.shape) >= 2:
            # Check if any two dimensions match lat/lon sizes
            lat_size, lon_size = len(ds.lat), len(ds.lon)
            shape = data_array.shape
            
            # For 3D: check if last two dims match lat,lon
            if len(shape) == 3 and (shape[-2:] == (lat_size, lon_size) or shape[-2:] == (lon_size, lat_size)):
                return True
            # For 2D: check if dims match lat,lon
            elif len(shape) == 2 and (shape == (lat_size, lon_size) or shape == (lon_size, lat_size)):
                return True
                
        return False
    
    # Create new dataset with remapped variables
    remapped_vars = {}
    
    for var_name in ds.data_vars:
        data_array = ds[var_name]
        
        if not should_remap_variable(var_name, data_array):
            print(f"Skipping variable: {var_name} (dims: {data_array.dims}, shape: {data_array.shape})")
            continue
            
        print(f"Remapping variable: {var_name}")
        
        # Apply remapping
        remapped_data = remap_variable(
            data_array,  # Pass the xarray DataArray
            data_array.values,  # Pass the numpy values
            weights.src_idx.values, 
            weights.weights.values, 
            weights.valid.values
        )
        
        if remapped_data is None:
            continue
        
        # Create new DataArray with cell dimension
        if len(data_array.dims) == 3:  # time, lat, lon -> time, cell
            new_dims = [data_array.dims[0], 'cell']
            new_coords = {data_array.dims[0]: data_array.coords[data_array.dims[0]]}
        elif len(data_array.dims) == 2:  # lat, lon -> cell
            new_dims = ['cell']
            new_coords = {}
        
        remapped_vars[var_name] = xr.DataArray(
            remapped_data,
            dims=new_dims,
            coords=new_coords,
            attrs=data_array.attrs
        )
    
    # Create new dataset
    ds_remap = xr.Dataset(remapped_vars, attrs=ds.attrs)
    
    # Add coordinates: cell and crs
    ds_remap = ds_remap.assign_coords(cell=np.arange(npix))
    
    # Add crs coordinate as a scalar variable with HEALPix grid mapping attributes
    crs_var = xr.DataArray(
        data=0,  # Scalar value
        dims=[],  # No dimensions (scalar)
        attrs={
            'grid_mapping_name': 'healpix',
            'healpix_nside': nside,
            'healpix_order': 'nest'
        }
    )
    ds_remap = ds_remap.assign_coords(crs=crs_var)
    
    # Update dataset attributes to include HEALPix information
    ds_remap.attrs.update({
        'healpix_order': zoom,
        'healpix_nside': nside,
        'healpix_npix': npix,
        'healpix_nest': True,
        'grid_mapping': 'crs'
    })
    
    return ds_remap


def remap_merg_to_healpix(input_file, output_file=None, zoom=9, weights_file=None, force_recompute=False):
    """
    Remap MERG dataset from lat/lon to HEALPix grid.
    
    Parameters:
    -----------
    input_file : str
        Path to input MERG NetCDF file
    output_file : str, optional
        Path to output file (default: adds _healpix_z{zoom} suffix)
    zoom : int, default=9
        HEALPix zoom level (order 9)
    weights_file : str or Path, optional
        Path to save/load weights NetCDF file
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
    """
    
    input_path = Path(input_file)
    if output_file is None:
        output_file = input_path.stem + f"_healpix_z{zoom}.nc"
    
    print(f"Reading {input_file}...")
    # Open with chunking for memory efficiency - only chunk time dimension
    ds = xr.open_dataset(input_file, chunks={'time': 1})
    
    print(f"Original dataset shape: {dict(ds.sizes)}")
    print(f"Longitude range: {ds.lon.min().values:.2f} to {ds.lon.max().values:.2f}")
    print(f"Latitude range: {ds.lat.min().values:.2f} to {ds.lat.max().values:.2f}")
    
    # Use the improved remapping function
    ds_remap = remap_latlon_to_healpix(ds, zoom, weights_file, force_recompute)
    
    print(f"Remapped dataset shape: {dict(ds_remap.sizes)}")
    
    # Add additional metadata for MERG dataset
    ds_remap.attrs.update({
        'original_grid': 'regular_lat_lon',
        'original_resolution': '0.01_degree',
        'remapping_method': 'delaunay_triangulation'
    })
    
    # Create output directory if it doesn't exist
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert boolean attributes to strings for NetCDF compatibility
    for attr_name, attr_value in ds_remap.attrs.items():
        if isinstance(attr_value, bool):
            ds_remap.attrs[attr_name] = str(attr_value)
    
    # Save with compression and proper encoding
    print(f"Writing to {output_file}...")
    encoding = {}
    for var_name in ds_remap.data_vars:
        encoding[var_name] = {
            'zlib': True, 
            'complevel': 4,
            'dtype': 'float32'  # Ensure consistent float32 type
        }

    # Add encoding for coordinates
    encoding['cell'] = {'dtype': 'int32'}
    # Note: crs coordinate with empty array shape (0,) doesn't need special encoding
    
    try:
        # Set time as unlimited dimension explicitly
        ds_remap.to_netcdf(output_file, encoding=encoding, unlimited_dims=['time'])
        print("File saved successfully!")
        
        # Print file size
        file_size_mb = output_path.stat().st_size / 1024**2
        print(f"Output file size: {file_size_mb:.1f} MB")
        
    except Exception as e:
        print(f"Error saving file: {e}")
        print("Dataset info for debugging:")
        print(f"Dataset dims: {ds_remap.dims}")
        for var in ds_remap.data_vars:
            print(f"{var}: shape={ds_remap[var].shape}, dtype={ds_remap[var].dtype}")
        raise
    
    print("Done!")
    return ds_remap


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python remap_merg_to_healpix.py <input_file> [output_file] [weights_file]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    weights_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    # If no weights file specified, create a default one based on input file
    if weights_file is None:
        input_path = Path(input_file)
        # Use the same weights directory structure as the notebook
        weights_dir = "/pscratch/sd/w/wcmca1/GPM/weights/"
        weights_file = f"{weights_dir}imerg_v07b_to_healpix_z9_weights.nc"
    
    ds_remap = remap_merg_to_healpix(input_file, output_file, zoom=9, weights_file=weights_file)
