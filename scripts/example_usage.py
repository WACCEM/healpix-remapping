#!/usr/bin/env python3
"""
Example usage of the generalized remap_to_zarr workflow.

This script demonstrates how to use the generalized workflow with different
input datasets that have different file naming conventions.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from remap_imerg_to_zarr import process_imerg_to_zarr

# =============================================================================
# Example 1: Original IMERG data format
# =============================================================================
# Filename: 3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4
# Directory structure: base_dir/YYYY/*.nc*
# Date in filename: YYYYMMDD (8 digits)

def process_imerg_example():
    """Process original IMERG data."""
    
    process_imerg_to_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
        zoom=9,
        output_zarr="/path/to/imerg_output.zarr",
        input_base_dir="/data/IMERG",
        
        # IMERG-specific parameters (these are the defaults)
        date_pattern=r'\.(\d{8})-',      # Match .YYYYMMDD-
        date_format='%Y%m%d',            # Parse YYYYMMDD
        use_year_subdirs=True,           # Look in yearly subdirectories
        file_glob='*.nc*',               # Match .nc, .nc4, etc.
        
        # Processing options
        time_average="1h",               # Average from 30min to 1h
        convert_time=True,               # Convert cftime to datetime64
        overwrite=True
    )


# =============================================================================
# Example 2: ir_imerg data format
# =============================================================================
# Filename: merg_2020123108_10km-pixel.nc
# Directory structure: base_dir/*.nc (flat, no yearly subdirs)
# Date in filename: YYYYMMDDhh (10 digits with hour)

def process_ir_imerg_example():
    """Process ir_imerg data with hourly timestamps in filename."""
    
    process_imerg_to_zarr(
        start_date=datetime(2020, 12, 31, 8),    # Include hour
        end_date=datetime(2020, 12, 31, 18),     # Include hour
        zoom=9,
        output_zarr="/path/to/ir_imerg_output.zarr",
        input_base_dir="/data/ir_imerg",
        
        # ir_imerg-specific parameters
        date_pattern=r'_(\d{10})_',      # Match _YYYYMMDDhh_
        date_format='%Y%m%d%H',          # Parse YYYYMMDDhh (includes hour)
        use_year_subdirs=False,          # Flat directory structure
        file_glob='*.nc',                # Match only .nc files
        
        # Processing options
        time_average=None,               # No temporal averaging needed
        convert_time=True,
        overwrite=True
    )


# =============================================================================
# Example 3: Generic data with different naming convention
# =============================================================================
# Filename: precipitation_2020-01-01.nc
# Directory structure: base_dir/*.nc (flat)
# Date in filename: YYYY-MM-DD with dashes

def process_generic_example():
    """Process data with YYYY-MM-DD format in filename."""
    
    process_imerg_to_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        zoom=9,
        output_zarr="/path/to/generic_output.zarr",
        input_base_dir="/data/generic",
        
        # Generic format parameters
        date_pattern=r'_(\d{4}-\d{2}-\d{2})',  # Match _YYYY-MM-DD
        date_format='%Y-%m-%d',                # Parse with dashes
        use_year_subdirs=False,
        file_glob='precipitation_*.nc',        # More specific glob
        
        # Processing options
        time_average="1d",               # Daily average
        convert_time=True,
        overwrite=True
    )


# =============================================================================
# Example 4: Data with timestamp at end of filename
# =============================================================================
# Filename: data.20200101.v2.nc
# Directory structure: base_dir/YYYY/*.nc
# Date in filename: YYYYMMDD (8 digits)

def process_timestamp_suffix_example():
    """Process data with date at different position in filename."""
    
    process_imerg_to_zarr(
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
        zoom=9,
        output_zarr="/path/to/suffix_output.zarr",
        input_base_dir="/data/suffix_format",
        
        # Custom pattern for suffix position
        date_pattern=r'\.(\d{8})\.',     # Match .YYYYMMDD.
        date_format='%Y%m%d',
        use_year_subdirs=True,
        file_glob='data.*.nc',
        
        # Processing options
        time_average="3h",
        convert_time=True,
        overwrite=True
    )


if __name__ == "__main__":
    # Uncomment the example you want to run
    
    # process_imerg_example()
    # process_ir_imerg_example()
    # process_generic_example()
    # process_timestamp_suffix_example()
    
    print("Select an example function to run and uncomment it in the main block.")
