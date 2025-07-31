#!/usr/bin/env python3
"""
Efficient script to remap multi-year IMERG datasets to HEALPix and save as Zarr.
Optimized for NERSC Perlmutter with Dask lazy evaluation and chunking.

Usage: python remap_imerg_to_zarr.py <start_year> <end_year> <zoom> [output_zarr_path]
Example: python remap_imerg_to_zarr.py 2019 2021 9 /pscratch/sd/w/wcmca1/GPM/IMERG_healpix_2019-2021_z9.zarr
"""

import sys
import numpy as np
import xarray as xr
import healpy as hp
import easygems.remap as egr
import dask
from dask.distributed import Client
from pathlib import Path
import glob
import logging
from datetime import datetime
import warnings
import re
import zarr
import remap_tools
import zarr_tools
import chunk_tools

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)


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


def setup_dask_client(n_workers=2, threads_per_worker=4, memory_limit='10GB'):
    """
    Set up Dask client optimized for high-resolution HEALPix processing.
    
    Parameters:
    -----------
    n_workers : int
        Number of Dask workers (very conservative for memory efficiency)
    threads_per_worker : int  
        Threads per worker (reduced for memory efficiency)
    memory_limit : str
        Memory limit per worker (very conservative for high-resolution data)
    """
    client = Client(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        silence_logs=logging.ERROR
    )
    logger.info(f"Dask client started: {client.dashboard_link}")
    return client


def get_imerg_files(start_year, end_year, base_dir="/pscratch/sd/w/wcmca1/GPM/IMERG_V07B_hpss/", 
                   start_date=None, end_date=None):
    """
    Get list of IMERG files for the specified year range and optionally filter by date.
    
    Parameters:
    -----------
    start_year : int
        Starting year (inclusive)
    end_year : int
        Ending year (inclusive)
    base_dir : str
        Base directory containing yearly subdirectories
    start_date : datetime, optional
        Filter files from this date (inclusive)
    end_date : datetime, optional
        Filter files to this date (inclusive)
        
    Returns:
    --------
    list : Sorted list of file paths
    """
    files = []
    for year in range(start_year, end_year + 1):
        year_dir = Path(base_dir) / str(year)
        if year_dir.exists():
            year_files = sorted(glob.glob(str(year_dir / "*.nc4")))
            
            # If date filtering is requested, filter files by date
            if start_date is not None or end_date is not None:
                filtered_files = []
                for file_path in year_files:
                    # Extract date from filename (assuming IMERG naming convention)
                    # Example: 3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4
                    try:
                        filename = Path(file_path).name
                        # Look for YYYYMMDD pattern in filename
                        import re
                        date_match = re.search(r'\.(\d{8})-', filename)
                        if date_match:
                            file_date_str = date_match.group(1)
                            file_date = datetime.strptime(file_date_str, '%Y%m%d')
                            
                            # Check if file date is within range
                            if start_date and file_date < start_date:
                                continue
                            if end_date and file_date > end_date:
                                continue
                            filtered_files.append(file_path)
                    except:
                        # If we can't parse the date, include the file to be safe
                        filtered_files.append(file_path)
                
                year_files = filtered_files
            
            files.extend(year_files)
            logger.info(f"Found {len(year_files)} files for year {year}")
        else:
            logger.warning(f"Directory not found: {year_dir}")
    
    logger.info(f"Total files found: {len(files)}")
    return files
    
    logger.info(f"Total files found: {len(files)}")
    return files


def gen_weights_latlon_optimized(lon, lat, zoom, weights_file=None, force_recompute=False):
    """
    Generate or load optimized remapping weights for lat/lon to HEALPix.
    
    Parameters:
    -----------
    lon : array-like
        1D longitude array in degrees
    lat : array-like
        1D latitude array in degrees  
    zoom : int
        HEALPix zoom level
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
    
    logger.info(f"Computing new weights for HEALPix zoom {zoom}")
    nside = 2**zoom  # healpy uses nside = 2^order for zoom level
    npix = hp.nside2npix(nside)
    
    # Get HEALPix pixel coordinates
    hp_lon, hp_lat = hp.pix2ang(
        nside=nside, ipix=np.arange(npix), lonlat=True, nest=True
    )
    
    # Create 2D meshgrid and flatten
    lon_2d, lat_2d = np.meshgrid(lon, lat)
    source_lon = lon_2d.flatten()
    source_lat = lat_2d.flatten()
    
    # Handle global periodicity
    logger.info("Handling longitude periodicity for global grid")
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
    
    # Add metadata
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


def process_imerg_to_zarr(start_year, end_year, zoom, output_zarr, 
                         weights_file=None, time_chunk_size=48,
                         start_date=None, end_date=None, input_base_dir=None, force_recompute=False):
    """
    Main function to process multi-year IMERG data to HEALPix Zarr following zarr_tools pattern.
    
    Parameters:
    -----------
    start_year : int
        Starting year
    end_year : int
        Ending year  
    zoom : int
        HEALPix zoom level (order)
    output_zarr : str
        Output Zarr path
    weights_file : str, optional
        Weights file path
    time_chunk_size : int
        Time chunk size for processing (default: 48 for 2 days)
    start_date : datetime, optional
        Filter files from this date (inclusive)
    end_date : datetime, optional
        Filter files to this date (inclusive)
    input_base_dir : str, optional
        Base directory for input files
    force_recompute : bool, default=False
        If True, recompute weights even if weights_file exists
    
    Note:
    -----
    Spatial chunk size is automatically computed based on zoom level using chunk_tools.compute_chunksize()
    """
    # Setup Dask client with conservative settings for high-resolution data
    client = setup_dask_client()
    
    try:
        # Get file list with optional date filtering
        base_dir = input_base_dir or "/pscratch/sd/w/wcmca1/GPM/IMERG_V07B_hpss/"
        files = get_imerg_files(start_year, end_year, base_dir, start_date, end_date)
        if not files:
            raise ValueError("No files found for the specified period")
        
        # Calculate optimal input chunking based on zoom level
        # Don't chunk during remapping - process full spatial grids at once
        # Only chunk time after remapping for zarr writing
        logger.info("Using full spatial chunks for remapping (no spatial chunking)")
        
        # Open multi-file dataset with time chunking for better parallelism
        logger.info("Opening multi-file dataset...")
        
        # Open with proper time chunking from the start
        ds = xr.open_mfdataset(
            files,
            parallel=True,
            decode_times=True,
            use_cftime=True,  # Use cftime to handle Julian calendar
            combine='by_coords',
            chunks={'time': time_chunk_size, 'lat': -1, 'lon': -1}  # Chunk time, keep spatial intact
        )
        
        logger.info(f"Dataset loaded: {ds.sizes}")
        logger.info(f"Time range: {ds.time.min().values} to {ds.time.max().values}")
        
        # Remap to HEALPix using remap_tools (following ICON pattern)
        logger.info(f"Remapping to HEALPix zoom level {zoom}")
        
        ds_remap = remap_tools.remap_delaunay(ds, zoom, weights_file, force_recompute)

        logger.info(f"Remapped dataset: {ds_remap.sizes}")
        logger.info(f"Variables: {list(ds_remap.data_vars)}")
        
        # Calculate optimal spatial chunk size based on zoom level
        spatial_chunk_size = chunk_tools.compute_chunksize(order=zoom)
        logger.info(f"Using spatial chunk size: {spatial_chunk_size} (zoom {zoom})")
        
        # Rechunk for optimal Zarr writing with proper chunk sizes
        logger.info(f"Rechunking for Zarr writing: time={time_chunk_size}, spatial={spatial_chunk_size}")
        ds_remap_chunked = ds_remap.chunk({
            'time': time_chunk_size,
            'cell': spatial_chunk_size  # Use optimal spatial chunking
        })
        
        # Use direct xarray to_zarr for better parallel performance
        logger.info(f"Writing to Zarr with parallel processing: {output_zarr}")
        output_path = Path(output_zarr)
        
        # Create encoding for efficient storage
        encoding = {
            'precipitation': {
                'compressor': zarr.Blosc(cname='zstd', clevel=3, shuffle=2),
                'chunks': (time_chunk_size, spatial_chunk_size),  # Time and spatial chunking
                'dtype': 'float32'
            },
            'time': {'dtype': 'datetime64[ns]'},
            'cell': {'dtype': 'int32'}
        }
        
        # Write to Zarr with compute=True for immediate parallel execution
        ds_remap_chunked.to_zarr(
            output_path,
            encoding=encoding,
            compute=True,  # Execute immediately with all available workers
            consolidated=True  # Optimize metadata access
        )
        
        logger.info(f"Successfully created Zarr dataset: {output_zarr}")
        logger.info(f"Final dataset shape: {ds_remap_chunked.sizes}")
        
        return ds_remap_chunked
        
    finally:
        client.close()


def main():
    if len(sys.argv) < 4:
        print("Usage: python remap_imerg_to_zarr.py <start_year> <end_year> <zoom> [output_zarr_path]")
        print("Example: python remap_imerg_to_zarr.py 2019 2021 9 /pscratch/sd/w/wcmca1/GPM/IMERG_healpix_2019-2021_z9.zarr")
        sys.exit(1)
    
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    zoom = int(sys.argv[3])
    
    if len(sys.argv) > 4:
        output_zarr = sys.argv[4]
    else:
        output_zarr = f"/pscratch/sd/w/wcmca1/GPM/IMERG_healpix_{start_year}-{end_year}_z{zoom}.zarr"
    
    # Default weights file
    weights_file = f"/pscratch/sd/w/wcmca1/GPM/weights/imerg_v07b_to_healpix_z{zoom}_weights.nc"
    
    logger.info(f"Processing IMERG data from {start_year} to {end_year}")
    logger.info(f"HEALPix zoom level: {zoom}")
    logger.info(f"Output Zarr: {output_zarr}")
    logger.info(f"Weights file: {weights_file}")
    
    # Process the data
    process_imerg_to_zarr(
        start_year=start_year,
        end_year=end_year,
        zoom=zoom,
        output_zarr=output_zarr,
        weights_file=weights_file
    )


if __name__ == "__main__":
    main()
