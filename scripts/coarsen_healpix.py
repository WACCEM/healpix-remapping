#!/usr/bin/env python3
"""
Coarsen HEALPix data to lower zoom levels.

This script takes processed HEALPix data at a high zoom level and 
progressively coarsens it to all lower zoom levels (down to zoom 0).
Each coarsening step reduces the resolution by a factor of 4 (2^2).

Works with any HEALPix Zarr dataset.

Usage:
    python coarsen_healpix.py INPUT_ZARR [options]
    
Examples:
    # Coarsen from zoom 9 down to zoom 5, output to same directory as input
    python coarsen_healpix.py IMERG_V7_1H_zoom9_20200101_20200131.zarr --target_zoom 5
    
    # Coarsen to zoom 0, specify output directory
    python coarsen_healpix.py data.zarr --output_dir /path/to/output
    
    # Apply temporal coarsening (1H -> 6H) using simple factor method
    python coarsen_healpix.py data.zarr --temporal_factor 6 --target_zoom 7
    
    # Apply temporal resampling to specific hours (works with any input times)
    python coarsen_healpix.py data.zarr --target_hours 0 6 12 18 --target_zoom 7
    
    # Use custom compression settings from config file
    python coarsen_healpix.py data.zarr --config my_config.yaml
"""

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
    if 'cell' in dataset.dims and 'cell' in dataset.coords:
        cell_var = dataset.coords['cell']
        if cell_var.dtype.kind == 'i':  # integer
            encoding['cell'] = {'dtype': 'int32'}
        elif cell_var.dtype.kind == 'f':  # floating point
            encoding['cell'] = {'dtype': 'float32'}
    
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


def coarsen_healpix_data(input_zarr, output_dir=None, target_zoom=0, temporal_factor=1, 
                         target_hours=None, time_chunk_size=24, compression_config=None, 
                         overwrite=False):
    """
    Coarsen HEALPix data from high zoom level to progressively lower levels.
    Optionally performs temporal coarsening (averaging) as well.
    
    Parameters:
    -----------
    input_zarr : str or Path
        Path to input Zarr file (highest zoom level)
    output_dir : str or Path, optional
        Output directory for coarsened files. If None, uses same directory as input.
    target_zoom : int
        Target zoom level to coarsen down to (default: 0)
    temporal_factor : int
        Temporal coarsening factor (default: 1, no temporal coarsening)
        e.g., 3 for 1H->3H, 6 for 1H->6H, 24 for 1H->1D
        Ignored if target_hours is provided.
    target_hours : list of int, optional
        Specific hours to align output times to (e.g., [0, 6, 12, 18] for 6-hourly).
        If provided, uses xarray.resample() for flexible time alignment.
        If None, uses simple coarsen() with temporal_factor (requires evenly divisible times).
    time_chunk_size : int
        Chunk size for time dimension (default: 24)
    compression_config : dict, optional
        Compression configuration. If None, uses default settings.
    overwrite : bool
        Whether to overwrite existing files (default: False)
    """
    
    # Extract information from input filename
    input_path = Path(input_zarr)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_zarr}")
    
    file_info = extract_info_from_filename(input_path.name)
    start_zoom = file_info['zoom']
    
    logger.info(f"Processing: {input_path.name}")
    logger.info(f"Starting zoom level: {start_zoom}")
    logger.info(f"Target zoom level: {target_zoom}")
    
    if start_zoom <= target_zoom:
        logger.warning(f"Start zoom ({start_zoom}) must be higher than target zoom ({target_zoom})")
        return
    
    # Load the highest resolution dataset
    logger.info(f"Loading dataset from: {input_zarr}")
    ds = xr.open_zarr(input_zarr)
    logger.info(f"Dataset loaded: {ds.sizes}")
    logger.info(f"Variables: {list(ds.data_vars)}")
    
    # Apply temporal averaging if requested
    if temporal_factor > 1 or target_hours is not None:
        ds, file_info = apply_temporal_averaging(ds, file_info, temporal_factor, target_hours)
    
    # Determine output directory
    if output_dir is None:
        # Use same directory as input
        output_dir = input_path.parent
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")
    
    # Start coarsening loop
    current_ds = ds
    
    for zoom_level in range(start_zoom - 1, target_zoom - 1, -1):
        logger.info(f"🔄 Coarsening to zoom level {zoom_level}...")
        
        # Generate output filename with time resolution preserved
        if file_info['time_resolution']:
            output_filename = f"{file_info['base_name']}_{file_info['time_resolution']}_zoom{zoom_level}_{file_info['start_date']}_{file_info['end_date']}.zarr"
        else:
            output_filename = f"{file_info['base_name']}_zoom{zoom_level}_{file_info['start_date']}_{file_info['end_date']}.zarr"
        output_path = output_dir / output_filename
        
        # Check if output already exists
        if output_path.exists() and not overwrite:
            logger.warning(f"Output file already exists (use --overwrite to replace): {output_path}")
            continue
        elif output_path.exists() and overwrite:
            logger.info(f"Removing existing file: {output_path}")
            shutil.rmtree(output_path)
        
        # Coarsen the dataset by factor of 4 (2^2)
        logger.info(f"   Coarsening {current_ds.sizes['cell']} cells to {current_ds.sizes['cell']//4} cells")
        
        # Use simple coarsening approach like SCREAM
        # This preserves the coordinate structure automatically
        coarsened_ds = current_ds.coarsen(cell=4).mean()
        
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
            'coarsened_from_zoom': zoom_level + 1,
            'coarsening_method': 'mean',
            'coarsening_factor': 4,
            'processing_timestamp': datetime.now().isoformat(),
            'source_file': str(input_path.name)
        }
        
        # Add temporal processing info if applicable
        if target_hours is not None:
            metadata_updates['temporal_resampling_method'] = 'resample'
            metadata_updates['temporal_target_hours'] = str(target_hours)
        elif temporal_factor > 1:
            metadata_updates['temporal_coarsening_factor'] = temporal_factor
            metadata_updates['temporal_resampling_method'] = 'coarsen'
        
        coarsened_ds.attrs.update(metadata_updates)
        
        # Prepare chunking (use provided time chunk size, compute spatial chunks efficiently)
        spatial_chunk_size = chunk_tools.compute_chunksize(zoom_level)
        
        chunked_ds = coarsened_ds.chunk({
            'time': time_chunk_size,
            'cell': spatial_chunk_size
        })
        
        # Write to Zarr with compression
        logger.info(f"   Writing to: {output_path}")
        store = zarr.storage.DirectoryStore(str(output_path), dimension_separator="/")
        
        encoding = get_encoding(chunked_ds, compression_config)
        chunked_ds.to_zarr(store, encoding=encoding, consolidated=True)
        
        logger.info(f"✅ Successfully wrote zoom {zoom_level}: {output_path}")
        logger.info(f"   Final size: {chunked_ds.sizes}")
        
        # Clean up and prepare for next iteration
        current_ds.close()
        del current_ds
        current_ds = coarsened_ds
        del store
        gc.collect()
        
        logger.info(f"   Memory cleanup completed")
    
    # Clean up final dataset
    current_ds.close()
    ds.close()
    
    logger.info("🎉 Coarsening completed successfully!")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Coarsen HEALPix data to lower zoom levels.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
    # Coarsen from zoom 9 down to zoom 5
    %(prog)s IMERG_V7_1H_zoom9_20200101_20200131.zarr --target_zoom 5
    
    # Coarsen to zoom 0, specify output directory
    %(prog)s /path/to/data.zarr --output_dir /path/to/output
    
    # Apply temporal coarsening (1H -> 6H) using simple factor method
    %(prog)s data.zarr --temporal_factor 6 --target_zoom 7
    
    # Resample to specific hours (more flexible, works with any input times)
    %(prog)s data.zarr --target_hours 0 6 12 18 --target_zoom 7
    
    # Use custom compression settings from config file
    %(prog)s data.zarr --config my_config.yaml --overwrite

    Temporal Averaging Methods:
    1. --temporal_factor: Simple integer factor (e.g., 6 for 1H->6H)
        - Requires evenly spaced input times
        - Uses xarray.coarsen() method
        - Works well when input starts at standard hours (0, 6, 12, 18)
    
    2. --target_hours: Resample to specific hours (e.g., 0 6 12 18)
        - Works with ANY input time structure
        - Uses xarray.resample() method
        - Preferred when input times don't align with standard hours
            """
    )
    
    # Required arguments
    parser.add_argument('input_zarr',
                        help='Path to input HEALPix Zarr file (highest zoom level)')
    
    # Optional arguments
    parser.add_argument('--output_dir', '-o',
                        help='Output directory for coarsened files. If not specified, '
                             'uses same directory as input file.')
    
    parser.add_argument('--target_zoom', '-z', type=int, default=0,
                        help='Target zoom level to coarsen down to (default: 0)')
    
    parser.add_argument('--temporal_factor', '-t', type=int, default=1,
                        help='Temporal coarsening factor (default: 1, no temporal coarsening). '
                             'E.g., 3 for 1H->3H, 6 for 1H->6H, 24 for 1H->1D. '
                             'Note: This requires evenly spaced times. '
                             'Ignored if --target_hours is specified.')
    
    parser.add_argument('--target_hours', type=int, nargs='+',
                        help='Specific hours of day to align output to (e.g., 0 6 12 18 for 6-hourly). '
                             'This method works with any input time structure and uses xarray.resample(). '
                             'If specified, --temporal_factor is ignored.')
    
    parser.add_argument('--time_chunk_size', type=int, default=24,
                        help='Chunk size for time dimension in output (default: 24)')
    
    parser.add_argument('--config', '-c',
                        help='Path to YAML configuration file for compression settings. '
                             'If not provided, uses default compression (zstd, level 3)')
    
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing output files')
    
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
    
    # Validate input file path
    input_path = Path(args.input_zarr)
    if not input_path.is_absolute():
        # If relative path, resolve from current directory
        input_path = Path.cwd() / args.input_zarr
    
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
    
    logger.info(f"Input file: {input_path}")
    logger.info(f"Target zoom level: {args.target_zoom}")
    
    # Log temporal processing options
    if args.target_hours:
        logger.info(f"Temporal resampling: target hours = {args.target_hours}")
        logger.info(f"  (Using xarray.resample() method - works with any time structure)")
    elif args.temporal_factor > 1:
        logger.info(f"Temporal coarsening factor: {args.temporal_factor}")
        logger.info(f"  (Using coarsen() method - requires evenly spaced times)")
    else:
        logger.info(f"No temporal averaging (temporal_factor=1)")
    
    logger.info(f"Time chunk size: {args.time_chunk_size}")
    logger.info(f"Overwrite existing files: {args.overwrite}")
    if args.output_dir:
        logger.info(f"Output directory: {args.output_dir}")
    else:
        logger.info(f"Output directory: same as input ({input_path.parent})")
    
    try:
        # Run the coarsening process
        coarsen_healpix_data(
            input_zarr=str(input_path),
            output_dir=args.output_dir,
            target_zoom=args.target_zoom,
            temporal_factor=args.temporal_factor,
            target_hours=args.target_hours,
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
