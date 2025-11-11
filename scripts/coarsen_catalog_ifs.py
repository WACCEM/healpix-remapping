#!/usr/bin/env python3
"""
Coarsen IFS HEALPix data to lower zoom level.

This script coarsens IFS HEALPix data from zoom level 11 and 
coarsens it to a specified lower zoom level (e.g., zoom 8).

Written specifically for IFS data from the online catalog.

Usage:
    python coarsen_catalog_ifs.py [options]
    
Examples:
    # Coarsen from zoom 11 down to zoom 8, output to same directory as input
    python coarsen_catalog_ifs.py --target_zoom 8
    
    # Apply temporal coarsening (1H -> 3H) using simple factor method
    python coarsen_catalog_ifs.py --temporal_factor 3 --target_zoom 8
    
    # Apply temporal resampling to specific hours (works with any input times)
    python coarsen_catalog_ifs.py --target_hours 0 6 12 18 --target_zoom 8
    
    # Apply coarsening to specific date range
    python coarsen_catalog_ifs.py --start_date 2020-01-11 --end_date 2021-03-01 \
        --target_zoom 8 --temporal_factor 3 --overwrite
"""

import os
import sys
import argparse
import yaml
import numpy as np
import xarray as xr
import pandas as pd
import zarr
import gc
import re
import shutil
from pathlib import Path
from datetime import datetime
import logging
import intake

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from src import chunk_tools

# Configure logging (will be set up in main function)
logger = None


def load_config(config_path):
    """
    Load configuration from YAML file.
    
    Parameters:
    -----------
    config_path : str or Path
        Path to configuration YAML file
        
    Returns:
    --------
    dict : Configuration dictionary
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_default_compression_config():
    """
    Get default compression configuration.
    
    Returns:
    --------
    dict : Default compression settings
    """
    return {
        'compressor': 'zstd',
        'compressor_level': 3,
        'dtype': 'float32'
    }


def get_encoding(dataset, compression_config=None):

    """
    Get optimized encoding for Zarr output based on configuration.
    
    Parameters:
    -----------
    dataset : xr.Dataset
        Dataset to encode
    compression_config : dict, optional
        Compression configuration dictionary with keys:
        - compressor: Compression algorithm (default: 'zstd')
        - compressor_level: Compression level (default: 3)
        - dtype: Data type for floating point variables (default: 'float32')
        If None, uses default compression settings.
        
    Returns:
    --------
    dict : Encoding dictionary for to_zarr()
    """
    if compression_config is None:
        compression_config = get_default_compression_config()
    
    encoding = {}
    
    # Apply encoding to all data variables in the dataset
    for var_name in dataset.data_vars:
        var = dataset[var_name]
        
        # Check if the variable has floating point data
        if var.dtype.kind == 'f':  # floating point
            encoding[var_name] = {
                'compressor': zarr.Blosc(
                    cname=compression_config.get('compressor', 'zstd'),
                    clevel=compression_config.get('compressor_level', 3),
                    shuffle=2
                ),
                'dtype': compression_config.get('dtype', 'float32')
            }
        else:
            # For non-floating point data, preserve the original dtype
            encoding[var_name] = {
                'compressor': zarr.Blosc(
                    cname=compression_config.get('compressor', 'zstd'),
                    clevel=compression_config.get('compressor_level', 3),
                    shuffle=2
                ),
                'dtype': var.dtype
            }
    
    # Set encoding for coordinate variables based on their actual data type
    # Handle all coordinate variables that have a 'cell' dimension
    for coord_name in dataset.coords:
        coord_var = dataset.coords[coord_name]
        
        # Skip time coordinate (handled differently)
        if coord_name == 'time':
            continue
            
        # For coordinates with 'cell' dimension, match the chunking
        if 'cell' in coord_var.dims:
            # Get the actual chunks from the variable if it's dask-backed
            if hasattr(coord_var.data, 'chunks'):
                # coord_var.data.chunks is a tuple of tuples, one per dimension
                # e.g., ((262144, 262144, 262144),) for a single dimension
                # We need to convert to a tuple of ints: (262144,)
                # by taking the first chunk size from each dimension
                actual_chunks = coord_var.data.chunks
                # Convert tuple of tuples to tuple of ints (first chunk size per dim)
                chunk_sizes = tuple(chunks[0] for chunks in actual_chunks)
                
                encoding[coord_name] = {
                    'chunks': chunk_sizes,
                    'compressor': zarr.Blosc(
                        cname=compression_config.get('compressor', 'zstd'),
                        clevel=compression_config.get('compressor_level', 3),
                        shuffle=2
                    )
                }
                if coord_var.dtype.kind == 'f':
                    encoding[coord_name]['dtype'] = 'float32'
            else:
                # Not chunked, just set dtype
                if coord_var.dtype.kind == 'i':
                    encoding[coord_name] = {'dtype': 'int32'}
                elif coord_var.dtype.kind == 'f':
                    encoding[coord_name] = {'dtype': 'float32'}
        elif coord_name == 'cell':
            # Special handling for cell coordinate itself
            if coord_var.dtype.kind == 'i':
                encoding[coord_name] = {'dtype': 'int32'}
            elif coord_var.dtype.kind == 'f':
                encoding[coord_name] = {'dtype': 'float32'}
    
    return encoding


def extract_info_from_filename(filename):
    """
    Extract information from IMERG HEALPix filename.
    
    Expected format: IMERG_V7_1H_zoom9_20200101_20200131.zarr
    
    Parameters:
    -----------
    filename : str
        Input filename
        
    Returns:
    --------
    dict : Dictionary with extracted information
    """
    stem = Path(filename).stem
    
    # Pattern to match IMERG filename format with time resolution
    pattern = r'(.+)_([0-9]+[HD])_zoom(\d+)_(\d{8})_(\d{8})'
    match = re.search(pattern, stem)
    
    if not match:
        # Fallback pattern without time resolution
        pattern = r'(.+)_zoom(\d+)_(\d{8})_(\d{8})'
        match = re.search(pattern, stem)
        if not match:
            raise ValueError(f"Cannot parse filename format: {filename}")
        
        base_name = match.group(1)  # e.g., "IMERG_V7"
        time_res = None
        zoom = int(match.group(2))  # e.g., 9
        start_date = match.group(3)  # e.g., "20200101"
        end_date = match.group(4)  # e.g., "20200131"
    else:
        base_name = match.group(1)  # e.g., "IMERG_V7"
        time_res = match.group(2)   # e.g., "1H"
        zoom = int(match.group(3))  # e.g., 9
        start_date = match.group(4)  # e.g., "20200101"
        end_date = match.group(5)   # e.g., "20200131"
    
    return {
        'base_name': base_name,
        'time_resolution': time_res,
        'zoom': zoom,
        'start_date': start_date,
        'end_date': end_date
    }


def apply_temporal_subsampling(ds, file_info, subsample_factor):
    """
    Subsample (select every Nth timestep) without averaging.
    Useful for instantaneous data where averaging is not needed.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with time dimension
    file_info : dict
        Dictionary with file information including 'time_resolution'
    subsample_factor : int
        Subsampling factor (e.g., 3 for selecting every 3rd timestep)
        
    Returns:
    --------
    tuple : (ds_subsampled, file_info_updated)
        - ds_subsampled: Dataset with subsampled time dimension
        - file_info_updated: Updated file_info dict with new time_resolution
    """
    # Make a copy of file_info to avoid modifying the original
    file_info = file_info.copy()
    
    logger.info(f"🕒 Applying temporal subsampling: factor {subsample_factor}")
    logger.info(f"   Original time dimension: {ds.sizes['time']} timesteps")
    logger.info(f"   Selecting every {subsample_factor} timestep (no averaging)")
    
    # Simple subsampling using isel with slice
    ds_subsampled = ds.isel(time=slice(None, None, subsample_factor))
    
    logger.info(f"   After subsampling: {ds_subsampled.sizes['time']} timesteps")
    logger.info(f"   First few times: {ds_subsampled.time.dt.strftime('%Y-%m-%d %H:%M').values[:min(4, ds_subsampled.sizes['time'])]}")
    
    # Update time resolution in filename info for output naming
    if file_info['time_resolution']:
        original_res = file_info['time_resolution']
        if original_res.endswith('H'):
            new_hours = int(original_res[:-1]) * subsample_factor
            if new_hours >= 24 and new_hours % 24 == 0:
                file_info['time_resolution'] = f"{new_hours // 24}D"
            else:
                file_info['time_resolution'] = f"{new_hours}H"
        elif original_res.endswith('D'):
            new_days = int(original_res[:-1]) * subsample_factor
            file_info['time_resolution'] = f"{new_days}D"
        logger.info(f"   Updated time resolution: {original_res} -> {file_info['time_resolution']}")
    
    return ds_subsampled, file_info


def subset_by_date_range(ds, start_date=None, end_date=None):
    """
    Subset dataset by date range.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with time dimension
    start_date : str, optional
        Start date (format: 'YYYY-MM-DD' or 'YYYYMMDD')
    end_date : str, optional
        End date (format: 'YYYY-MM-DD' or 'YYYYMMDD')
        
    Returns:
    --------
    xr.Dataset : Subsetted dataset
    """
    if start_date is None and end_date is None:
        return ds
    
    logger.info(f"📅 Subsetting by date range...")
    logger.info(f"   Original time range: {ds.time.min().dt.strftime('%Y-%m-%d').item()} to {ds.time.max().dt.strftime('%Y-%m-%d').item()}")
    logger.info(f"   Original time dimension: {ds.sizes['time']} timesteps")
    
    # Parse dates (handle both YYYY-MM-DD and YYYYMMDD formats)
    def parse_date(date_str):
        if date_str is None:
            return None
        # Remove hyphens if present
        date_str = date_str.replace('-', '')
        # Parse as YYYYMMDD
        return pd.to_datetime(date_str, format='%Y%m%d')
    
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    
    # For end_date, set to 23:59:59 to include the full day but exclude the next day's 00:00
    if end_dt is not None:
        end_dt = end_dt + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    
    # Build selection string
    if start_dt is not None and end_dt is not None:
        logger.info(f"   Selecting: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d %H:%M:%S')} (inclusive, excludes next day's 00:00)")
        ds_subset = ds.sel(time=slice(start_dt, end_dt))
    elif start_dt is not None:
        logger.info(f"   Selecting: from {start_dt.strftime('%Y-%m-%d')} onwards")
        ds_subset = ds.sel(time=slice(start_dt, None))
    elif end_dt is not None:
        logger.info(f"   Selecting: up to {end_dt.strftime('%Y-%m-%d %H:%M:%S')} (inclusive, excludes next day's 00:00)")
        ds_subset = ds.sel(time=slice(None, end_dt))
    
    logger.info(f"   After subsetting: {ds_subset.sizes['time']} timesteps")
    if ds_subset.sizes['time'] > 0:
        logger.info(f"   New time range: {ds_subset.time.min().dt.strftime('%Y-%m-%d').item()} to {ds_subset.time.max().dt.strftime('%Y-%m-%d').item()}")
    else:
        logger.warning(f"   ⚠️  No data found in specified date range!")
    
    return ds_subset


def apply_temporal_averaging(ds, file_info, temporal_factor=1, target_hours=None):
    """
    Apply temporal averaging to dataset using either resample or coarsen method.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with time dimension
    file_info : dict
        Dictionary with file information including 'time_resolution'
    temporal_factor : int
        Temporal coarsening factor for coarsen() method (default: 1)
    target_hours : list of int, optional
        Specific hours to align output times to for resample() method
        
    Returns:
    --------
    tuple : (ds_averaged, file_info_updated)
        - ds_averaged: Dataset with temporal averaging applied
        - file_info_updated: Updated file_info dict with new time_resolution
    """
    # Make a copy of file_info to avoid modifying the original
    file_info = file_info.copy()
    
    if target_hours is not None:
        # Use resample() method for flexible time alignment to specific hours
        logger.info(f"🕒 Applying temporal resampling with target hours: {target_hours}")
        logger.info(f"   Original time dimension: {ds.sizes['time']} timesteps")
        
        # Determine output frequency from target hours
        if len(target_hours) < 2:
            raise ValueError("target_hours must contain at least 2 hours")
        
        hour_diff = target_hours[1] - target_hours[0]
        output_freq = f'{hour_diff}h'
        
        # Get base hour for offset
        base_hour = target_hours[0]
        offset_str = f'{base_hour}h'
        
        logger.info(f"   Using resample: freq={output_freq}, offset={offset_str}")
        logger.info(f"   This method works regardless of input time alignment")
        
        # Use xarray's resample method (handles any input time structure)
        ds_resampled = ds.resample(
            time=output_freq,
            offset=offset_str,
            label='left',      # Label with the left (start) edge of each interval
            closed='left'      # Left-closed intervals: [start, end)
        ).mean()
        
        # Verify alignment
        sample_times = pd.DatetimeIndex(ds_resampled.time.values)
        output_hours_actual = sorted(sample_times.hour.unique())
        logger.info(f"   After resampling: {ds_resampled.sizes['time']} timesteps")
        logger.info(f"   Output hours: {output_hours_actual}")
        logger.info(f"   First few times: {sample_times[:min(4, len(sample_times))].strftime('%Y-%m-%d %H:%M').tolist()}")
        
        # Check alignment
        misaligned = [h for h in output_hours_actual if h not in target_hours]
        if misaligned:
            logger.warning(f"   ⚠️  Some output hours not in target list: {misaligned}")
        else:
            logger.info(f"   ✓ All output times aligned to target hours!")
        
        # Update time resolution for filename
        if file_info['time_resolution']:
            original_res = file_info['time_resolution']
            file_info['time_resolution'] = f"{hour_diff}H"
            logger.info(f"   Updated time resolution: {original_res} -> {file_info['time_resolution']}")
        else:
            file_info['time_resolution'] = f"{hour_diff}H"
            logger.info(f"   Set time resolution: {file_info['time_resolution']}")
        
        return ds_resampled, file_info
        
    elif temporal_factor > 1:
        # Use coarsen() method for simple integer factor averaging
        logger.info(f"🕒 Applying temporal coarsening: factor {temporal_factor}")
        logger.info(f"   Original time dimension: {ds.sizes['time']} timesteps")
        logger.info(f"   Note: This method requires evenly spaced times starting from standard hours")
        
        # Check if temporal coarsening is possible
        if ds.sizes['time'] % temporal_factor != 0:
            logger.warning(f"Time dimension ({ds.sizes['time']}) not evenly divisible by temporal_factor ({temporal_factor})")
            logger.warning(f"Will truncate to {(ds.sizes['time'] // temporal_factor) * temporal_factor} timesteps")
            # Truncate to make it evenly divisible
            ds = ds.isel(time=slice(0, (ds.sizes['time'] // temporal_factor) * temporal_factor))
        
        # Perform temporal coarsening using xarray's coarsen
        ds_coarsened = ds.coarsen(time=temporal_factor, boundary='trim').mean()
        
        # Fix time coordinates to use beginning of time window instead of center
        # Get the original time coordinates for the beginning of each window
        original_times = ds.time.values
        window_starts = original_times[::temporal_factor]  # Every temporal_factor-th time step
        
        # Ensure we have the right number of time coordinates
        n_coarse_times = ds_coarsened.sizes['time']
        if len(window_starts) >= n_coarse_times:
            new_times = window_starts[:n_coarse_times]
        else:
            # This shouldn't happen with boundary='trim', but just in case
            new_times = window_starts
        
        # Assign the corrected time coordinates
        ds_coarsened = ds_coarsened.assign_coords(time=new_times)
        logger.info(f"   After temporal coarsening: {ds_coarsened.sizes['time']} timesteps")
        logger.info(f"   Time coordinates corrected to window start times")
        logger.info(f"   First few corrected times: {ds_coarsened.time.dt.strftime('%Y-%m-%d %H:%M').values[:4]}")
        
        # Update time resolution in filename info for output naming
        if file_info['time_resolution']:
            original_res = file_info['time_resolution']
            if original_res.endswith('H'):
                new_hours = int(original_res[:-1]) * temporal_factor
                if new_hours >= 24 and new_hours % 24 == 0:
                    file_info['time_resolution'] = f"{new_hours // 24}D"
                else:
                    file_info['time_resolution'] = f"{new_hours}H"
            elif original_res.endswith('D'):
                new_days = int(original_res[:-1]) * temporal_factor
                file_info['time_resolution'] = f"{new_days}D"
            logger.info(f"   Updated time resolution: {original_res} -> {file_info['time_resolution']}")
        
        return ds_coarsened, file_info
    
    else:
        # No temporal averaging requested
        return ds, file_info


def coarsen_healpix_data(ds, start_zoom, output_dir, target_zoom=0, temporal_factor=1, 
                         target_hours=None, time_subsample_factor=1, time_chunk_size=24, 
                         compression_config=None, overwrite=False):
    """
    Coarsen HEALPix data from high zoom level to progressively lower levels.
    Optionally performs temporal coarsening (averaging) or subsampling.
    
    Parameters:
    -----------
    ds : xarray.Dataset
        Input dataset (highest zoom level)
    start_zoom : int
        Input zoom level of the dataset
    output_dir : str or Path
        Output directory for coarsened files.
    target_zoom : int
        Target zoom level to coarsen down to (default: 0)
    temporal_factor : int
        Temporal coarsening factor for AVERAGING (default: 1, no temporal coarsening)
        e.g., 3 for 1H->3H, 6 for 1H->6H, 24 for 1H->1D
        Ignored if target_hours or time_subsample_factor is provided.
    target_hours : list of int, optional
        Specific hours to align output times to (e.g., [0, 6, 12, 18] for 6-hourly).
        If provided, uses xarray.resample() for flexible time alignment with AVERAGING.
        If specified, time_subsample_factor and temporal_factor are ignored.
    time_subsample_factor : int
        Temporal subsampling factor for SELECTING every Nth timestep WITHOUT averaging (default: 1)
        e.g., 3 for selecting every 3rd timestep from 1H to 3H instantaneous data
        Ignored if target_hours is provided. Takes precedence over temporal_factor.
    time_chunk_size : int
        Chunk size for time dimension (default: 24)
    compression_config : dict, optional
        Compression configuration. If None, uses default settings.
    overwrite : bool
        Whether to overwrite existing files (default: False)
    """
    
    # logger.info(f"Processing: {input_path.name}")
    logger.info(f"Starting zoom level: {start_zoom}")
    logger.info(f"Target zoom level: {target_zoom}")
    
    if start_zoom <= target_zoom:
        logger.warning(f"Start zoom ({start_zoom}) must be higher than target zoom ({target_zoom})")
        return
    
    # Get start and end dates from time coordinate
    start_date = ds.time.min().dt.strftime('%Y%m%d').item()
    end_date = ds.time.max().dt.strftime('%Y%m%d').item()
    # Make file_info dictionary (hardcoded for this example)
    file_info = {
        'base_name': 'ifs_tco3999_rcbmf',
        'time_resolution': '1H',
        'zoom': start_zoom,
        'start_date': start_date,
        'end_date': end_date
    }
    
    # Apply temporal processing if requested
    # Priority: target_hours > time_subsample_factor > temporal_factor
    if target_hours is not None:
        # Resample with averaging to specific hours
        ds, file_info = apply_temporal_averaging(ds, file_info, temporal_factor, target_hours)
    elif time_subsample_factor > 1:
        # Subsample (select every Nth timestep) without averaging
        ds, file_info = apply_temporal_subsampling(ds, file_info, time_subsample_factor)
    elif temporal_factor > 1:
        # Coarsen with averaging
        ds, file_info = apply_temporal_averaging(ds, file_info, temporal_factor, target_hours=None)
    
    # Determine output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")
    
    # Direct coarsening to target zoom (no intermediate levels)
    current_ds = ds
    zoom_level = target_zoom
    
    logger.info(f"🔄 Direct coarsening from zoom {start_zoom} to zoom level {zoom_level}...")
    
    # Generate output filename with time resolution preserved
    if file_info['time_resolution']:
        output_filename = f"{file_info['base_name']}_{file_info['time_resolution']}_zoom{zoom_level}_{file_info['start_date']}_{file_info['end_date']}.zarr"
    else:
        output_filename = f"{file_info['base_name']}_zoom{zoom_level}_{file_info['start_date']}_{file_info['end_date']}.zarr"
    output_path = output_dir / output_filename

    # Check if output already exists
    if output_path.exists() and not overwrite:
        logger.warning(f"Output file already exists (use --overwrite to replace): {output_path}")
        logger.info("Exiting without processing.")
        return
    elif output_path.exists() and overwrite:
        logger.info(f"Removing existing file: {output_path}")
        shutil.rmtree(output_path)
    
    # Calculate coarsening factor for direct coarsening
    # Each zoom level reduces resolution by factor of 4 (2^2)
    zoom_diff = start_zoom - zoom_level
    coarsen_factor = 4 ** zoom_diff
    
    logger.info(f"   Zoom difference: {zoom_diff}, coarsening factor: {coarsen_factor}")
    logger.info(f"   Coarsening {current_ds.sizes['cell']} cells to {current_ds.sizes['cell']//coarsen_factor} cells")
    
    # Use simple coarsening approach with calculated factor
    # This preserves the coordinate structure automatically
    coarsened_ds = current_ds.coarsen(cell=coarsen_factor).mean()
    
    # Ensure cell coordinate remains integer type (critical for HEALPix compatibility)
    n_cells_coarse = coarsened_ds.sizes['cell']
    coarsened_ds = coarsened_ds.assign_coords(
        cell=np.arange(n_cells_coarse, dtype='int64')
    )

    # Update HEALPix metadata
    if 'crs' in coarsened_ds:
        coarsened_ds['crs'].attrs['healpix_nside'] = 2**zoom_level
        coarsened_ds['crs'].attrs['healpix_order'] = zoom_level
        logger.info(f"   Updated HEALPix nside to: {2**zoom_level}")

    # Add processing metadata
    metadata_updates = {
        'coarsened_from_zoom': start_zoom,
        'coarsening_method': 'mean',
        'coarsening_factor': coarsen_factor,
        'processing_script': os.path.basename(__file__),
        'processing_timestamp': datetime.now().isoformat(),
        'source_catalog': f"{file_info['base_name']}_zoom{start_zoom}",
    }
    
    # Add temporal processing info if applicable
    if target_hours is not None:
        metadata_updates['temporal_resampling_method'] = 'resample'
        metadata_updates['temporal_target_hours'] = str(target_hours)
    elif time_subsample_factor > 1:
        metadata_updates['temporal_subsample_factor'] = time_subsample_factor
        metadata_updates['temporal_resampling_method'] = 'subsample'
    elif temporal_factor > 1:
        metadata_updates['temporal_coarsening_factor'] = temporal_factor
        metadata_updates['temporal_resampling_method'] = 'coarsen'
    
    coarsened_ds.attrs.update(metadata_updates)
    
    # Prepare chunking (use provided time chunk size, compute spatial chunks efficiently)
    spatial_chunk_size = chunk_tools.compute_chunksize(zoom_level)
    
    # Chunk the dataset - this will rechunk data variables and coordinates consistently
    chunked_ds = coarsened_ds.chunk({
        'time': time_chunk_size,
        'cell': spatial_chunk_size
    })
    
    # Ensure all coordinate variables with 'cell' dimension are also properly chunked
    # This is critical to avoid encoding/chunk mismatch errors
    coords_to_rechunk = {}
    for coord_name in chunked_ds.coords:
        if 'cell' in chunked_ds.coords[coord_name].dims and coord_name != 'cell':
            coords_to_rechunk[coord_name] = chunked_ds.coords[coord_name].chunk({'cell': spatial_chunk_size})
    
    if coords_to_rechunk:
        chunked_ds = chunked_ds.assign_coords(coords_to_rechunk)
        logger.info(f"   Rechunked coordinate variables: {list(coords_to_rechunk.keys())}")
    
    # Write to Zarr with compression
    logger.info(f"   Writing to: {output_path}")
    store = zarr.storage.DirectoryStore(str(output_path), dimension_separator="/")
    
    encoding = get_encoding(chunked_ds, compression_config)
    chunked_ds.to_zarr(store, encoding=encoding, consolidated=True)
    
    logger.info(f"✅ Successfully wrote zoom {zoom_level}: {output_path}")
    logger.info(f"   Final size: {chunked_ds.sizes}")
    
    # Clean up
    current_ds.close()
    coarsened_ds.close()
    ds.close()
    del store
    gc.collect()
    
    logger.info("🎉 Coarsening completed successfully!")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Coarsen HEALPix data to lower zoom levels.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
    # Coarsen from zoom 9 down to zoom 5
    %(prog)s --target_zoom 5
    
    # Coarsen to zoom 0, specify output directory
    %(prog)s --output_dir /path/to/output
    
    # Apply temporal coarsening (1H -> 6H) using simple factor method WITH AVERAGING
    %(prog)s --temporal_factor 6 --target_zoom 7
    
    # Subsample time (1H -> 3H) WITHOUT AVERAGING (for instantaneous data)
    %(prog)s --time_subsample_factor 3 --target_zoom 7
    
    # Resample to specific hours (more flexible, works with any input times) WITH AVERAGING
    %(prog)s --target_hours 0 6 12 18 --target_zoom 7
    
    # Use custom compression settings from config file
    %(prog)s --config my_config.yaml --overwrite

    Temporal Processing Methods:
    1. --temporal_factor: Simple integer factor WITH AVERAGING (e.g., 6 for 1H->6H averaged)
        - Requires evenly spaced input times
        - Uses xarray.coarsen() method with mean()
        - Works well when input starts at standard hours (0, 6, 12, 18)
    
    2. --target_hours: Resample to specific hours WITH AVERAGING (e.g., 0 6 12 18)
        - Works with ANY input time structure
        - Uses xarray.resample() method with mean()
        - Preferred when input times don't align with standard hours
    
    3. --time_subsample_factor: Subsample WITHOUT AVERAGING (e.g., 3 for every 3rd timestep)
        - Selects every Nth timestep using isel(time=slice(None, None, N))
        - No averaging performed - preserves instantaneous values
        - Ideal for instantaneous data like snapshots or diagnostic output
            """
    )
    
    # Required arguments
    
    # Optional arguments   
    parser.add_argument('--target_zoom', '-z', type=int, default=0,
                        help='Target zoom level to coarsen down to (default: 0)')
    
    parser.add_argument('--temporal_factor', '-t', type=int, default=1,
                        help='Temporal coarsening factor WITH AVERAGING (default: 1, no temporal coarsening). '
                             'E.g., 3 for 1H->3H, 6 for 1H->6H, 24 for 1H->1D. '
                             'Note: This requires evenly spaced times. '
                             'Ignored if --target_hours or --time_subsample_factor is specified.')
    
    parser.add_argument('--target_hours', type=int, nargs='+',
                        help='Specific hours of day to align output to WITH AVERAGING (e.g., 0 6 12 18 for 6-hourly). '
                             'This method works with any input time structure and uses xarray.resample(). '
                             'If specified, --time_subsample_factor and --temporal_factor are ignored.')
    
    parser.add_argument('--time_subsample_factor', type=int, default=1,
                        help='Temporal subsampling factor WITHOUT AVERAGING (default: 1, no subsampling). '
                             'Selects every Nth timestep. E.g., 3 for selecting every 3rd hour (1H->3H instantaneous). '
                             'Useful for instantaneous data where averaging is not appropriate. '
                             'Ignored if --target_hours is specified. Takes precedence over --temporal_factor.')
    
    parser.add_argument('--time_chunk_size', type=int, default=24,
                        help='Chunk size for time dimension in output (default: 24)')
    
    parser.add_argument('--config', '-c',
                        help='Path to YAML configuration file for compression settings. '
                             'If not provided, uses default compression (zstd, level 3)')
    
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing output files')
    
    parser.add_argument('--start_date', type=str,
                        help='Start date for time subsetting (format: YYYY-MM-DD or YYYYMMDD). '
                             'If specified, only process data from this date onwards.')
    
    parser.add_argument('--end_date', type=str,
                        help='End date for time subsetting (format: YYYY-MM-DD or YYYYMMDD). '
                             'If specified, only process data up to and including this date.')
    
    return parser.parse_args()


def main():
    """Main function to handle command line arguments and run coarsening."""
    
    # Set up logging
    global logger
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True
    )
    logger = logging.getLogger()
    
    # Parse arguments
    args = parse_arguments()
    
    # Load configuration if provided
    compression_config = None
    if args.config:
        try:
            config = load_config(args.config)
            compression_config = config.get('compression', None)
            logger.info(f"Configuration loaded from: {args.config}")
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {args.config}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            sys.exit(1)
    else:
        logger.info("Using default compression settings (no config file provided)")

    # Specify catalog information
    catalog_file = "https://digital-earths-global-hackathon.github.io/catalog/catalog.yaml"
    catalog_location = "online"
    catalog_source = "ifs_tco3999_rcbmf"
    catalog_params = {
        "zoom": 11,
        "time": "PT1H",
    }
    input_zoom = catalog_params['zoom']

    # Specify output directory
    output_dir = "/pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf/"

    # Dictionary mapping input variable names to standard output names
    varout_dict = {
        'time': 'time', 
        'lat': 'lat', 
        'lon': 'lon', 
        'crs': 'crs',
        'level': 'lev', 
        'value': 'cell',
        'u': 'ua',                  # zonal wind
        'v': 'va',                  # meridional wind
        'w': 'omega',               # vertical velocity (pressure velocity)
        'z': 'z',                   # geopotential
        't': 'ta',                  # temperature
        'q': 'hus',                 # specific humidity
        'r': 'hur',                 # relative humidity
        'tp': 'pr',                 # total precipitation
        'tcwv': 'prw',              # precipitable water vapor
        'sp': 'ps',                 # surface pressure
        'msl': 'psl',               # mean sea level pressure
        '10u': 'uas',               # 10m u wind
        '10v': 'vas',               # 10m v wind
        '2t': 'tas',                # 2m temperature
        'ttr': 'rlut',              # TOA outgoing longwave radiation
        'slhf': 'hflsd',            # surface latent heat flux
        'sshf': 'hflsu',            # surface sensible heat flux
        # 'lsp': 'prs',             # large scale precipitation (not consistent with other models)
        # 'orog': 'ELEV',           # NOT available
        # 'sfcWind': 'sfcWind',     # NOT available
        # 'zg': 'zg',               # NOT available
    }

    # Load the HEALPix catalog
    print(f"Loading HEALPix catalog: {catalog_file}")
    in_catalog = intake.open_catalog(catalog_file)
    if catalog_location:
        in_catalog = in_catalog[catalog_location]

    # Get the DataSet from the catalog
    logger.info(f"Loading source: {catalog_source}")
    ds = in_catalog[catalog_source](**catalog_params).to_dask()
    logger.info(f"Dataset loaded: {ds.sizes}")
    logger.info(f"Variables: {list(ds.data_vars)}")

    # Subset variables and rename
    if varout_dict is not None:
        logger.info(f"Subsetting and renaming variables...")
        ds = ds[list(varout_dict.keys())]
        ds = ds.rename(varout_dict)
        logger.info(f"Variables after subsetting: {list(ds.data_vars)}")
    
    # Apply date range subsetting if requested (before any other processing)
    if args.start_date or args.end_date:
        ds = subset_by_date_range(ds, args.start_date, args.end_date)
        if ds.sizes['time'] == 0:
            logger.error("No data in specified date range. Exiting.")
            sys.exit(1)
    
    import pdb; pdb.set_trace()

    # Log temporal processing options
    if args.target_hours:
        logger.info(f"Temporal resampling WITH AVERAGING: target hours = {args.target_hours}")
        logger.info(f"  (Using xarray.resample() method - works with any time structure)")
    elif args.time_subsample_factor > 1:
        logger.info(f"Temporal subsampling WITHOUT AVERAGING: factor = {args.time_subsample_factor}")
        logger.info(f"  (Selecting every {args.time_subsample_factor} timestep)")
    elif args.temporal_factor > 1:
        logger.info(f"Temporal coarsening WITH AVERAGING: factor = {args.temporal_factor}")
        logger.info(f"  (Using coarsen() method - requires evenly spaced times)")
    else:
        logger.info(f"No temporal processing")
    
    logger.info(f"Time chunk size: {args.time_chunk_size}")
    logger.info(f"Overwrite existing files: {args.overwrite}")
    
    try:
        # Run the coarsening process
        coarsen_healpix_data(
            ds=ds,
            start_zoom=input_zoom,
            output_dir=output_dir,
            target_zoom=args.target_zoom,
            temporal_factor=args.temporal_factor,
            target_hours=args.target_hours,
            time_subsample_factor=args.time_subsample_factor,
            time_chunk_size=args.time_chunk_size,
            compression_config=compression_config,
            overwrite=args.overwrite
        )
        
        logger.info("All processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during coarsening: {e}")
        raise


if __name__ == "__main__":
    main()
