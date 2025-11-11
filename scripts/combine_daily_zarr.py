#!/usr/bin/env python3
"""
Combine daily Zarr files into a single Zarr file.

This script concatenates multiple daily Zarr files along the time dimension
while preserving all variable attributes, global attributes, chunking, and
compression settings from the original files.

Usage:
    python combine_daily_zarr.py --input_dir <path> --output_file <path> [options]
    
Examples:
    # Combine all files in a directory
    python combine_daily_zarr.py \
        --input_dir /pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf \
        --output_file /pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf_combined.zarr
    
    # Combine files for a specific date range
    python combine_daily_zarr.py \
        --input_dir /pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf \
        --output_file combined_jan2020.zarr \
        --start_date 2020-01-01 \
        --end_date 2020-01-31
    
    # Use custom file pattern
    python combine_daily_zarr.py \
        --input_dir /path/to/data \
        --output_file combined.zarr \
        --pattern "ifs_tco3999_rcbmf_3H_zoom8_*.zarr"
"""

import argparse
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
import xarray as xr
import zarr
import numpy as np
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def find_daily_zarr_files(input_dir, pattern="*.zarr", start_date=None, end_date=None):
    """
    Find all daily Zarr files in the input directory.
    
    Parameters:
    -----------
    input_dir : Path
        Directory containing daily Zarr files
    pattern : str
        File pattern to match (default: "*.zarr")
    start_date : str, optional
        Start date (format: YYYY-MM-DD)
    end_date : str, optional
        End date (format: YYYY-MM-DD)
        
    Returns:
    --------
    list : Sorted list of Zarr file paths
    """
    input_path = Path(input_dir)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    
    # Find all matching zarr directories
    zarr_files = sorted(input_path.glob(pattern))
    
    if not zarr_files:
        raise FileNotFoundError(f"No Zarr files found matching pattern: {pattern}")
    
    # Filter by date range if specified
    if start_date or end_date:
        filtered_files = []
        
        for zarr_file in zarr_files:
            # Extract date from filename (assumes format: *_YYYYMMDD_YYYYMMDD.zarr)
            try:
                parts = zarr_file.stem.split('_')
                # Look for 8-digit date pattern
                file_date = None
                for part in parts:
                    if len(part) == 8 and part.isdigit():
                        file_date = datetime.strptime(part, '%Y%m%d')
                        break
                
                if file_date is None:
                    logger.warning(f"Could not extract date from filename: {zarr_file.name}")
                    continue
                
                # Check if file is within date range
                if start_date:
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                    if file_date < start_dt:
                        continue
                
                if end_date:
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    if file_date > end_dt:
                        continue
                
                filtered_files.append(zarr_file)
                
            except Exception as e:
                logger.warning(f"Error processing filename {zarr_file.name}: {e}")
                continue
        
        zarr_files = sorted(filtered_files)
    
    logger.info(f"Found {len(zarr_files)} Zarr files to combine")
    
    return zarr_files


def get_encoding_from_zarr(ds, zarr_path):
    """
    Extract encoding information from an existing Zarr file.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Dataset opened from Zarr
    zarr_path : Path
        Path to the Zarr directory
        
    Returns:
    --------
    dict : Encoding dictionary for all variables
    """
    encoding = {}
    
    # Open the zarr store directly to get chunk and compression info
    store = zarr.open(str(zarr_path), mode='r')
    
    # Get encoding for each variable
    for var_name in ds.data_vars:
        if var_name in store:
            z_array = store[var_name]
            
            encoding[var_name] = {
                'chunks': z_array.chunks,
                'compressor': z_array.compressor,
                'dtype': z_array.dtype,
            }
            
            # Add filters if present
            if z_array.filters is not None:
                encoding[var_name]['filters'] = z_array.filters
    
    # Get encoding for coordinate variables
    for coord_name in ds.coords:
        if coord_name in store:
            z_array = store[coord_name]
            
            encoding[coord_name] = {
                'chunks': z_array.chunks,
                'compressor': z_array.compressor,
                'dtype': z_array.dtype,
            }
            
            if z_array.filters is not None:
                encoding[coord_name]['filters'] = z_array.filters
    
    return encoding


def combine_zarr_files(zarr_files, output_file, overwrite=False):
    """
    Combine multiple daily Zarr files into a single Zarr file.
    
    Parameters:
    -----------
    zarr_files : list
        List of paths to daily Zarr files
    output_file : Path
        Output Zarr file path
    overwrite : bool
        Whether to overwrite existing output file
    """
    # Check if output exists
    if output_file.exists():
        if overwrite:
            logger.info(f"Removing existing output: {output_file}")
            import shutil
            shutil.rmtree(output_file)
        else:
            raise FileExistsError(f"Output file already exists: {output_file}")
    
    logger.info(f"Combining {len(zarr_files)} files into: {output_file}")
    
    # Load first file to get encoding and attributes
    logger.info(f"Reading reference file: {zarr_files[0].name}")
    ds_first = xr.open_zarr(zarr_files[0])
    
    # Get encoding from first file
    logger.info("Extracting encoding information...")
    encoding = get_encoding_from_zarr(ds_first, zarr_files[0])
    
    # Store global attributes
    global_attrs = dict(ds_first.attrs)
    
    # Store variable attributes
    var_attrs = {}
    for var_name in ds_first.data_vars:
        var_attrs[var_name] = dict(ds_first[var_name].attrs)
    
    for coord_name in ds_first.coords:
        var_attrs[coord_name] = dict(ds_first[coord_name].attrs)
    
    ds_first.close()
    
    logger.info("Opening all files with xarray...")
    logger.info("This may take a moment for large datasets...")
    
    # Open all files with xarray (lazy loading)
    datasets = []
    for zarr_file in tqdm(zarr_files, desc="Loading files"):
        try:
            ds = xr.open_zarr(zarr_file)
            datasets.append(ds)
        except Exception as e:
            logger.error(f"Error opening {zarr_file.name}: {e}")
            raise
    
    # Concatenate along time dimension
    logger.info("Concatenating datasets along time dimension...")
    ds_combined = xr.concat(datasets, dim='time')
    
    # Restore attributes (concat may lose some attributes)
    logger.info("Restoring attributes...")
    ds_combined.attrs.update(global_attrs)
    
    for var_name in ds_combined.data_vars:
        if var_name in var_attrs:
            ds_combined[var_name].attrs.update(var_attrs[var_name])
    
    for coord_name in ds_combined.coords:
        if coord_name in var_attrs:
            ds_combined[coord_name].attrs.update(var_attrs[coord_name])
    
    # Update global attributes with processing info
    ds_combined.attrs['combined_from_files'] = f"{len(zarr_files)} daily files"
    ds_combined.attrs['combination_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ds_combined.attrs['combination_script'] = os.path.basename(__file__)
    ds_combined.attrs['time_range'] = f"{ds_combined.time.min().dt.strftime('%Y-%m-%d').item()} to {ds_combined.time.max().dt.strftime('%Y-%m-%d').item()}"
    
    # Write to output
    logger.info(f"Writing combined dataset to: {output_file}")
    logger.info(f"  Time dimension: {ds_combined.sizes['time']} timesteps")
    logger.info(f"  Time range: {ds_combined.attrs['time_range']}")
    logger.info(f"  Variables: {list(ds_combined.data_vars.keys())}")
    
    # Write with original encoding
    ds_combined.to_zarr(
        output_file,
        mode='w',
        encoding=encoding,
        consolidated=True
    )
    
    logger.info("✅ Successfully combined Zarr files!")
    
    # Close all datasets
    for ds in datasets:
        ds.close()
    ds_combined.close()
    
    # Validate output
    logger.info("Validating output file...")
    ds_check = xr.open_zarr(output_file)
    logger.info(f"  Output dimensions: {dict(ds_check.sizes)}")
    logger.info(f"  Output variables: {list(ds_check.data_vars.keys())}")
    logger.info(f"  Time coverage: {ds_check.time.min().dt.strftime('%Y-%m-%d %H:%M').item()} to {ds_check.time.max().dt.strftime('%Y-%m-%d %H:%M').item()}")
    
    # Check for monotonic time
    time_diff = ds_check.time.diff('time')
    if (time_diff < 0).any():
        logger.warning("⚠️  Time dimension is not monotonically increasing!")
    else:
        logger.info("  ✓ Time dimension is monotonically increasing")
    
    # Check for duplicate times
    time_values = ds_check.time.values
    if len(time_values) != len(np.unique(time_values)):
        logger.warning("⚠️  Duplicate timestamps detected!")
    else:
        logger.info("  ✓ No duplicate timestamps")
    
    ds_check.close()


def main():
    parser = argparse.ArgumentParser(
        description='Combine daily Zarr files into a single Zarr file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Required arguments
    parser.add_argument('--input_dir', required=True,
                        help='Directory containing daily Zarr files')
    parser.add_argument('--output_file', required=True,
                        help='Output Zarr file path')
    
    # Optional arguments
    parser.add_argument('--pattern', default='*.zarr',
                        help='File pattern to match (default: *.zarr)')
    parser.add_argument('--start_date',
                        help='Start date for filtering files (format: YYYY-MM-DD)')
    parser.add_argument('--end_date',
                        help='End date for filtering files (format: YYYY-MM-DD)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing output file')
    
    args = parser.parse_args()
    
    try:
        # Find input files
        zarr_files = find_daily_zarr_files(
            args.input_dir,
            pattern=args.pattern,
            start_date=args.start_date,
            end_date=args.end_date
        )
        
        if not zarr_files:
            logger.error("No files found to combine!")
            return 1
        
        # Show files to be combined
        logger.info(f"\nFiles to combine:")
        for i, f in enumerate(zarr_files[:5], 1):
            logger.info(f"  {i}. {f.name}")
        if len(zarr_files) > 5:
            logger.info(f"  ... and {len(zarr_files) - 5} more")
        logger.info("")
        
        # Combine files
        output_path = Path(args.output_file)
        combine_zarr_files(zarr_files, output_path, overwrite=args.overwrite)
        
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
