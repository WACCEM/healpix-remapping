#!/usr/bin/env python3
"""
Script to remap regular lat/lon grid data to HEALPix grid using easygems.remap
Adapted from the unstructured grid examples to work with regular grids.
"""

import numpy as np
import xarray as xr
import healpix as hp
import easygems.remap as egr
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def save_weights(weights, weights_file):
    """
    Save remapping weights to NetCDF file.
    
    Parameters:
    -----------
    weights : Dataset
        Remapping weights from easygems.remap.compute_weights_delaunay
    weights_file : str or Path
        Path to save weights NetCDF file
    """
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
    """
    Load remapping weights from NetCDF file.
    
    Parameters:
    -----------
    weights_file : str or Path
        Path to weights NetCDF file
        
    Returns:
    --------
    weights : Dataset
        Remapping weights for use with easygems.remap.apply_weights
    """
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


def gen_weights_latlon(lon, lat, order, weights_file=None, force_recompute=False):
    """
    Generate remapping weights from regular lat/lon grid to HEALPix grid.
    
    Parameters:
    -----------
    lon : array-like
        1D array of longitude values in degrees
    lat : array-like  
        1D array of latitude values in degrees
    order : int
        HEALPix order (zoom level)
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
                logger.warning(f"Error loading weights: {e}, recomputing...")
    
    logger.info(f"Computing new weights for regular lat/lon grid to HEALPix order {order}")
    nside = hp.order2nside(order)
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
        logger.info("  Handling longitude periodicity for global grid")
        
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
        logger.info("  Regional grid detected, no periodicity handling")
        weights = egr.compute_weights_delaunay(
            points=(source_lon, source_lat), 
            xi=(hp_lon, hp_lat)
        )
    
    # Add metadata about the grid configuration
    weights.attrs.update({
        'healpix_order': order,
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


def remap_latlon_to_healpix(ds: xr.Dataset, order: int, weights_file=None, force_recompute=False) -> xr.Dataset:
    """
    Remap a dataset from regular lat/lon grid to HEALPix grid.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with lat/lon coordinates
    order : int
        HEALPix order (zoom level)
    weights_file : str or Path, optional
        Path to save/load weights NetCDF file
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
        
    Returns:
    --------
    ds_remap : xr.Dataset
        Dataset remapped to HEALPix grid with 'cell' dimension
    """
    
    # Check that required coordinates exist
    if 'lat' not in ds.coords or 'lon' not in ds.coords:
        raise ValueError("Dataset must have 'lat' and 'lon' coordinates")
    
    # Generate remapping weights
    logger.info(f"Remapping to HEALPix order {order}")
    weights = gen_weights_latlon(ds.lon.values, ds.lat.values, order, weights_file, force_recompute)
    
    # Get number of HEALPix pixels
    npix = len(weights.tgt_idx)
    logger.info(f"Remapping to {npix} HEALPix pixels")
    
    # Apply remapping using xr.apply_ufunc
    ds_remap = xr.apply_ufunc(
        egr.apply_weights,
        ds,
        kwargs=weights,
        keep_attrs=True,
        input_core_dims=[["lat", "lon"]],  # Input dimensions to remap
        output_core_dims=[["cell"]],       # Output HEALPix dimension
        on_missing_core_dim="copy",        # Copy other dimensions as-is
        output_dtypes=["f4"],
        vectorize=True,
        dask="parallelized",
        dask_gufunc_kwargs={
            "output_sizes": {"cell": npix},
        },
    )
    
    return ds_remap


def remap_file(input_file, output_file, order=9, chunks=None, weights_file=None, force_recompute=False):
    """
    Remap a NetCDF file from lat/lon to HEALPix grid.
    
    Parameters:
    -----------
    input_file : str or Path
        Path to input NetCDF file
    output_file : str or Path  
        Path to output NetCDF file
    order : int, default=9
        HEALPix order (zoom level)
    chunks : dict, optional
        Chunking specification for dask arrays
    weights_file : str or Path, optional
        Path to save/load weights NetCDF file
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
    """
    
    input_file = Path(input_file)
    output_file = Path(output_file)
    
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    logger.info(f"Reading input file: {input_file}")
    
    # Open dataset with optional chunking
    if chunks is not None:
        ds = xr.open_dataset(input_file, chunks=chunks)
    else:
        ds = xr.open_dataset(input_file)
    
    logger.info(f"Input dataset shape: {dict(ds.sizes)}")
    
    # Perform remapping
    logger.info(f"Remapping to HEALPix order {order}")
    ds_remap = remap_latlon_to_healpix(ds, order, weights_file, force_recompute)
    
    # Add HEALPix metadata
    ds_remap.attrs.update({
        'healpix_order': order,
        'healpix_nside': hp.order2nside(order),
        'healpix_npix': hp.nside2npix(hp.order2nside(order)),
        'healpix_nest': True,
        'original_grid': 'lat_lon',
        'remapping_method': 'delaunay_triangulation'
    })
    
    logger.info(f"Output dataset shape: {dict(ds_remap.sizes)}")
    
    # Save to file
    logger.info(f"Writing output file: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Use compression for better storage efficiency
    encoding = {}
    for var in ds_remap.data_vars:
        if ds_remap[var].dtype.kind == 'f':  # Float variables
            encoding[var] = {'zlib': True, 'complevel': 4}
    
    ds_remap.to_netcdf(output_file, encoding=encoding)
    logger.info("Remapping completed successfully")
    
    return ds_remap


def main():
    """
    Example usage of the remapping functionality.
    Modify the file paths and parameters as needed.
    """
    
    # Example file paths - modify these for your data
    input_file = "merg_2020080620_4km-pixel.nc"  # Your input file
    output_file = "merg_2020080620_4km-pixel_healpix_z9.nc"  # Output file
    
    # Remapping parameters
    order = 9  # HEALPix zoom level 9
    
    # Optional: Define chunking for large datasets
    chunks = {
        'time': 1,     # Process one time step at a time
        'lat': 900,    # Half the lat dimension for memory efficiency  
        'lon': 1800    # Half the lon dimension for memory efficiency
    }
    
    # Define weights file for reuse
    weights_file = "merg_to_healpix_z9_weights.nc"
    
    try:
        # Perform the remapping
        ds_remap = remap_file(
            input_file=input_file,
            output_file=output_file,
            order=order,
            chunks=chunks,
            weights_file=weights_file,
            force_recompute=False  # Set to True to force recomputation
        )
        
        print(f"Successfully remapped {input_file} to {output_file}")
        print(f"Output dataset: {ds_remap}")
        
    except Exception as e:
        logger.error(f"Error during remapping: {e}")
        raise


if __name__ == "__main__":
    main()
