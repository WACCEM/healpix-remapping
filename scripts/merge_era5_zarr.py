#!/usr/bin/env python
"""
Merge ERA5 2D and 3D HEALPix Zarr files.

This script combines separate 2D surface variable and 3D pressure level variable
Zarr stores into a single unified dataset. This allows processing 2D and 3D variables
separately (which may have different time ranges, processing requirements, etc.) and
then combining them for analysis.

Example usage:
    # Basic merge
    python scripts/merge_era5_zarr.py \
        era5_2d_zoom8_20200101_20200131.zarr \
        era5_3d_zoom8_20200101_20200131.zarr \
        -o era5_combined_zoom8_20200101_20200131.zarr
    
    # Merge with validation
    python scripts/merge_era5_zarr.py file_2d.zarr file_3d.zarr -o combined.zarr --validate
    
    # Overwrite existing output
    python scripts/merge_era5_zarr.py file_2d.zarr file_3d.zarr -o combined.zarr --overwrite
    
    # Use more workers for faster parallel writing
    python scripts/merge_era5_zarr.py file_2d.zarr file_3d.zarr -o combined.zarr --n-workers 16
    
    # Disable parallel writing (single-threaded)
    python scripts/merge_era5_zarr.py file_2d.zarr file_3d.zarr -o combined.zarr --no-parallel
"""

import os
import sys
import argparse
import xarray as xr
from pathlib import Path
import shutil
import traceback

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utilities import setup_dask_client
from dask.diagnostics import ProgressBar

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Merge ERA5 2D and 3D HEALPix Zarr files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge 2D and 3D files (parallel writing with 8 workers)
  %(prog)s era5_2d_zoom8.zarr era5_3d_zoom8.zarr -o era5_combined_zoom8.zarr
  
  # Merge with validation
  %(prog)s file_2d.zarr file_3d.zarr -o combined.zarr --validate
  
  # Use 16 workers for faster writing
  %(prog)s file_2d.zarr file_3d.zarr -o combined.zarr --n-workers 16
  
  # Disable parallel writing
  %(prog)s file_2d.zarr file_3d.zarr -o combined.zarr --no-parallel
        """
    )
    
    parser.add_argument('file_2d', type=str,
                       help='Path to 2D surface variables Zarr store')
    parser.add_argument('file_3d', type=str,
                       help='Path to 3D pressure level variables Zarr store')
    parser.add_argument('-o', '--output', type=str, required=True,
                       help='Output path for merged Zarr store')
    parser.add_argument('--validate', action='store_true',
                       help='Perform validation checks before merging')
    parser.add_argument('--overwrite', action='store_true',
                       help='Overwrite output if it already exists')
    parser.add_argument('--no-chunking', action='store_true',
                       help='Disable rechunking of merged dataset')
    parser.add_argument('--n-workers', type=int, default=8,
                       help='Number of Dask workers for parallel writing (default: 8)')
    parser.add_argument('--no-parallel', action='store_true',
                       help='Disable parallel writing (use single-threaded)')
    
    return parser.parse_args()


def validate_datasets(ds_2d, ds_3d):
    """
    Validate that two datasets can be safely merged.
    
    Parameters
    ----------
    ds_2d : xr.Dataset
        2D surface variables dataset
    ds_3d : xr.Dataset
        3D pressure level variables dataset
        
    Returns
    -------
    bool
        True if validation passes
        
    Raises
    ------
    ValueError
        If validation fails with details about the issue
    """
    print("\nRunning validation checks...")
    print("-" * 60)
    
    # Check 1: Compatible time coordinates
    if 'time' not in ds_2d.sizes or 'time' not in ds_3d.sizes:
        raise ValueError("Both datasets must have 'time' dimension")
    
    # Check time ranges
    time_2d_start = ds_2d.time.values[0]
    time_2d_end = ds_2d.time.values[-1]
    time_3d_start = ds_3d.time.values[0]
    time_3d_end = ds_3d.time.values[-1]
    
    print(f"2D time range: {time_2d_start} to {time_2d_end}")
    print(f"3D time range: {time_3d_start} to {time_3d_end}")
    
    # Check if time ranges overlap
    if time_2d_end < time_3d_start or time_3d_end < time_2d_start:
        raise ValueError("Time ranges do not overlap!")
    
    # Check time lengths
    if len(ds_2d.time) != len(ds_3d.time):
        print(f"WARNING: Time dimensions have different lengths:")
        print(f"  2D: {len(ds_2d.time)} timesteps")
        print(f"  3D: {len(ds_3d.time)} timesteps")
        print("  Merge will use outer join (union of times)")
    
    # Check 2: Compatible spatial coordinates (cell dimension for HEALPix)
    if 'cell' not in ds_2d.sizes or 'cell' not in ds_3d.sizes:
        raise ValueError("Both datasets must have 'cell' dimension (HEALPix)")
    
    if len(ds_2d.cell) != len(ds_3d.cell):
        raise ValueError(
            f"Cell dimensions must match! 2D has {len(ds_2d.cell)}, "
            f"3D has {len(ds_3d.cell)} cells"
        )
    
    print(f"✓ Compatible spatial grid: {len(ds_2d.cell)} cells")
    
    # Check 3: No overlapping data variables
    vars_2d = set(ds_2d.data_vars)
    vars_3d = set(ds_3d.data_vars)
    overlap = vars_2d & vars_3d
    
    if overlap:
        raise ValueError(
            f"Datasets have overlapping variables: {overlap}\n"
            "Cannot merge datasets with duplicate variable names!"
        )
    
    print(f"✓ No overlapping variables")
    print(f"  2D variables ({len(vars_2d)}): {', '.join(sorted(vars_2d))}")
    print(f"  3D variables ({len(vars_3d)}): {', '.join(sorted(vars_3d))}")
    
    # Check 4: Check for pressure level dimension in 3D
    if 'lev' in ds_3d.sizes:
        print(f"✓ 3D dataset has pressure levels: {len(ds_3d.lev)} levels")
    else:
        print("WARNING: 3D dataset does not have 'lev' dimension")
    
    print("-" * 60)
    print("Validation passed!\n")
    return True


def merge_datasets(ds_2d, ds_3d, rechunk=True):
    """
    Merge 2D and 3D datasets.
    
    Parameters
    ----------
    ds_2d : xr.Dataset
        2D surface variables dataset
    ds_3d : xr.Dataset
        3D pressure level variables dataset
    rechunk : bool
        If True, rechunk merged dataset for optimal storage
        
    Returns
    -------
    xr.Dataset
        Merged dataset
    """
    print("Merging datasets...")
    
    # Merge with outer join to handle any time mismatches
    # compat='override' allows merging even with slight coordinate differences
    ds_merged = xr.merge([ds_2d, ds_3d], compat='override', join='outer')
    
    print(f"Merged dataset dimensions: {dict(ds_merged.sizes)}")
    print(f"Total variables: {len(ds_merged.data_vars)}")
    
    # Rechunk for optimal Zarr storage
    if rechunk:
        print("\nRechunking for optimal Zarr storage...")
        chunks = {
            'time': 24,  # Daily chunks
            'cell': -1,  # Full spatial
        }
        
        # Add pressure level chunking if present
        if 'lev' in ds_merged.sizes:
            chunks['lev'] = -1  # Keep all levels together
        
        ds_merged = ds_merged.chunk(chunks)
        print(f"Chunk sizes: {chunks}")
    
    return ds_merged


def main():
    """Main merge function."""
    args = parse_args()
    
    print("ERA5 HEALPix Zarr Merge Utility")
    print("=" * 60)
    
    # Setup Dask client for parallel writing (unless disabled)
    client = None
    if not args.no_parallel:
        print(f"\n🚀 Setting up Dask client with {args.n_workers} workers...")
        client = setup_dask_client(n_workers=args.n_workers, threads_per_worker=1)
        print(f"   Dashboard: {client.dashboard_link}")
    
    # Check input files exist
    if not os.path.exists(args.file_2d):
        print(f"ERROR: 2D file not found: {args.file_2d}")
        return 1
    if not os.path.exists(args.file_3d):
        print(f"ERROR: 3D file not found: {args.file_3d}")
        return 1
    
    # Check output doesn't exist (unless overwrite flag set)
    if os.path.exists(args.output) and not args.overwrite:
        print(f"ERROR: Output already exists: {args.output}")
        print("Use --overwrite to replace existing output")
        return 1
    
    print(f"\nInput files:")
    print(f"  2D: {args.file_2d}")
    print(f"  3D: {args.file_3d}")
    print(f"Output:")
    print(f"  {args.output}")
    
    try:
        # Open datasets
        print("\nOpening 2D dataset...")
        ds_2d = xr.open_zarr(args.file_2d)
        print(f"  Dimensions: {dict(ds_2d.sizes)}")
        print(f"  Variables: {list(ds_2d.data_vars)}")
        
        print("\nOpening 3D dataset...")
        ds_3d = xr.open_zarr(args.file_3d)
        print(f"  Dimensions: {dict(ds_3d.sizes)}")
        print(f"  Variables: {list(ds_3d.data_vars)}")
        
        # Validate if requested
        if args.validate:
            validate_datasets(ds_2d, ds_3d)
        
        # Merge datasets
        ds_merged = merge_datasets(ds_2d, ds_3d, rechunk=not args.no_chunking)
        
        # Write output
        print(f"\nWriting merged dataset to: {args.output}")
        
        # Remove output if it exists (for overwrite)
        if os.path.exists(args.output) and args.overwrite:
            print(f"Removing existing output: {args.output}")
            shutil.rmtree(args.output)
        
        # Write to Zarr with parallel execution if enabled
        if client:
            print("\n💾 Writing merged dataset to Zarr (parallel mode)...")
            print(f"   Dataset size: {ds_merged.nbytes / 1024**3:.2f} GB")
            
            # Create delayed write task
            write_task = ds_merged.to_zarr(args.output, mode='w', compute=False, consolidated=True)
            
            # Execute with progress bar
            print("   Executing parallel write...")
            with ProgressBar():
                write_task.compute()
        else:
            print("\n💾 Writing merged dataset to Zarr (sequential mode)...")
            ds_merged.to_zarr(args.output, mode='w', consolidated=True)
        
        print("\n" + "=" * 60)
        print("Merge completed successfully!")
        print(f"Output written to: {args.output}")
        print(f"Total variables: {len(ds_merged.data_vars)}")
        print(f"Dimensions: {dict(ds_merged.sizes)}")
        
        return 0
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"ERROR during merge: {e}")
        traceback.print_exc()
        return 1
    
    finally:
        # Clean up Dask client
        if client:
            print("\n🔄 Shutting down Dask cluster...")
            try:
                client.close()
            except:
                pass


if __name__ == '__main__':
    sys.exit(main())
