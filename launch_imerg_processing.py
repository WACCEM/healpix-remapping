#!/usr/bin/env python3
"""
Simple launcher script for IMERG to HEALPix processing
Usage: python launch_imerg_processing.py <start_date> <end_date> [zoom_level]
Example: python launch_imerg_processing.py 2020-01-01 2020-12-31 9
"""

import sys
import yaml
from pathlib import Path
from datetime import datetime
from remap_imerg_to_zarr import process_imerg_to_zarr

def load_config(config_path="imerg_config.yaml"):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")

def main():
    if len(sys.argv) < 3:
        print("Usage: python launch_imerg_processing.py <start_date> <end_date> [zoom_level] [--overwrite]")
        print("Example: python launch_imerg_processing.py 2020-01-01 2020-12-31 9")
        print("Example: python launch_imerg_processing.py 2020-01-01 2020-12-31 9 --overwrite")
        sys.exit(1)
    
    # Parse command line arguments
    start_date = parse_date(sys.argv[1])
    end_date = parse_date(sys.argv[2])
    
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
    config = load_config()
    
    # Use config defaults or command line values
    zoom = zoom or config['default_zoom']
    time_average = config.get('time_average')
    
    # Create output filename with time averaging info
    date_range = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    if time_average:
        # Convert time averaging to short format for filename
        time_suffix = time_average.upper().replace('H', 'H').replace('D', 'D')
        output_file = f"{config['output_base_dir']}/imerg_healpix_{time_suffix}_zoom{zoom}_{date_range}.zarr"
    else:
        output_file = f"{config['output_base_dir']}/imerg_healpix_zoom{zoom}_{date_range}.zarr"
    
    print(f"Processing IMERG data from {start_date.date()} to {end_date.date()}")
    print(f"HEALPix zoom level: {zoom}")
    if time_average:
        print(f"Temporal averaging: {time_average}")
    else:
        print("Temporal averaging: None (30-minute resolution)")
    print(f"Output file: {output_file}")
    if overwrite:
        print("⚠️  Overwrite mode enabled - existing files will be replaced")
    
    # Create weights file path
    weights_file = f"{config['weights_dir']}/imerg_v07b_to_healpix_z{zoom}_weights.nc"
    
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
        # force_recompute=True  # Force regeneration of weights to match new dataset structure
    )
    
    print(f"\nProcessing completed successfully!")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    main()
