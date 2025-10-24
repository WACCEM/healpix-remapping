#!/usr/bin/env python3
"""
Simple launcher script for IMERG to HEALPix processing

This script processes IMERG data files with flexible file pattern matching.
It reads configuration from imerg_config.yaml including file search patterns.

Usage: 
    python launch_imerg_processing.py <start_date> <end_date> [zoom_level] [--overwrite]
Examples:
    python launch_imerg_processing.py 2020-01-01 2020-12-31 9
    python launch_imerg_processing.py 2020-01-01 2020-01-31 9 --overwrite

Configuration:
    Edit config/tb_imerg_config.yaml to configure:
    - Input/output paths
    - File search patterns (date_pattern, date_format, use_year_subdirs, file_glob)
    - Processing parameters (zoom level, time averaging, chunking)
    - Dask cluster settings
"""

import sys
import yaml
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from remap_imerg_to_zarr import process_imerg_to_zarr
from src.utilities import parse_date

def load_config(config_path="imerg_config.yaml"):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def main():
    if len(sys.argv) < 3:
        print("Usage: python launch_imerg_processing.py <start_date> <end_date> [zoom_level] [--overwrite]")
        print("\nDate formats:")
        print("  YYYY-MM-DD       - Date only (end_date extends to 23:59:59)")
        print("  YYYY-MM-DDTHH    - Date with hour (end_date extends to HH:59:59)")
        print("\nExamples:")
        print("  python launch_imerg_processing.py 2020-01-01 2020-12-31 9")
        print("  python launch_imerg_processing.py 2020-01-01T00 2020-12-31T23 9")
        print("  python launch_imerg_processing.py 2020-01-01 2020-12-31 9 --overwrite")
        sys.exit(1)
    
    # Parse command line arguments with end-of-day extension
    start_date = parse_date(sys.argv[1], is_end_date=False)
    end_date = parse_date(sys.argv[2], is_end_date=True)
    
    # Get config path relative to script location
    script_dir = Path(__file__).parent
    config_path = script_dir.parent / "config" / "imerg_config.yaml"
    
    # Parse zoom level (optional)
    zoom = None
    overwrite = False
    
    # Process remaining arguments
    for arg in sys.argv[3:]:
        if arg == '--overwrite':
            overwrite = True
        else:
            try:
                zoom = int(arg)
            except ValueError:
                print(f"Warning: Ignoring unknown argument: {arg}")
    
    # Load configuration
    config = load_config(config_path=str(config_path))
    
    # Create output directory if it doesn't exist
    output_dir = Path(config['output_base_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Use config defaults or command line values
    zoom = zoom or config['default_zoom']
    time_average = config.get('time_average')
    output_basename = config.get('output_basename', 'IMERG')

    # Create output filename with time averaging info
    date_range = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    if time_average:
        # Convert time averaging to short format for filename
        time_suffix = time_average.upper().replace('H', 'H').replace('D', 'D')
        filename = f"{output_basename}_{time_suffix}_zoom{zoom}_{date_range}.zarr"
    else:
        filename = f"{output_basename}_zoom{zoom}_{date_range}.zarr"
    
    # Use pathlib to properly construct the path (handles extra slashes automatically)
    output_file = str(Path(config['output_base_dir']) / filename)

    print(f"Processing IMERG data from {start_date.date()} to {end_date.date()}")
    print(f"HEALPix zoom level: {zoom}")
    if time_average:
        print(f"Temporal averaging: {time_average}")
    else:
        print("Temporal averaging: None (30-minute resolution)")
    print(f"Output file: {output_file}")
    if overwrite:
        print("⚠️  Overwrite mode enabled - existing files will be replaced")
    
    # Create weights file path (use pathlib to handle extra slashes)
    weights_file = str(Path(config['weights_dir']) / f"imerg_v07b_to_healpix_z{zoom}_weights.nc")
    
    # Run the processing with correct parameters
    process_imerg_to_zarr(
        start_date=start_date,
        end_date=end_date,
        zoom=zoom,
        output_zarr=output_file,
        weights_file=weights_file,
        time_chunk_size=config['time_chunk_size'],
        input_base_dir=config['input_base_dir'],
        overwrite=overwrite,
        time_average=time_average,
        convert_time=config.get('convert_time', False),
        dask_config=config.get('dask', {}),
        force_recompute=config.get('force_recompute', False),
    )
    
    print(f"\nProcessing completed successfully!")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    main()
