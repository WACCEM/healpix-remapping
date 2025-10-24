#!/usr/bin/env python3
"""
Simple launcher script for IR+IMERG to HEALPix processing

This script processes IR+IMERG data files with flexible file pattern matching.
It reads configuration from tb_imerg_config.yaml including file search patterns.

Usage: 
    python launch_ir_imerg_processing.py <start_date> <end_date> [zoom_level] [--overwrite]

Examples:
    python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9
    python launch_ir_imerg_processing.py 2020-01-01 2020-01-31 9 --overwrite

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
from remap_imerg_to_zarr import process_imerg_to_zarr
from src.utilities import parse_date

def load_config(config_path="tb_imerg_config.yaml"):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def main():
    if len(sys.argv) < 3:
        print("Usage: python launch_ir_imerg_processing.py <start_date> <end_date> [zoom_level] [--overwrite]")
        print("\nDate formats:")
        print("  YYYY-MM-DD       - Date only (end_date extends to 23:59:59)")
        print("  YYYY-MM-DDTHH    - Date with hour (end_date extends to HH:59:59)")
        print("  YYYY-MM-DD HH    - Date with hour, space separated")
        print("\nExamples:")
        print("  python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9")
        print("  python launch_ir_imerg_processing.py 2020-01-01T00 2020-01-03T23 9")
        print("  python launch_ir_imerg_processing.py '2020-01-01 00' '2020-01-03 23' 9")
        print("  python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9 --overwrite")
        sys.exit(1)
    
    # Parse command line arguments
    start_date = parse_date(sys.argv[1], is_end_date=False)
    end_date = parse_date(sys.argv[2], is_end_date=True)
    config_path = "/global/homes/f/feng045/program/hackathon/remap_imerg/config/tb_imerg_config.yaml"
    
    # Parse zoom level (optional)
    zoom = None
    overwrite = True
    
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
    config = load_config(config_path=config_path)
    
    # Create output directory if it doesn't exist
    output_dir = Path(config['output_base_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Use config defaults or command line values
    zoom = zoom or config['default_zoom']
    time_average = config.get('time_average')
    output_basename = config.get('output_basename', 'IR_IMERG_V7')

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

    print(f"\n{'='*70}")
    print(f"Processing IR+IMERG data")
    print(f"{'='*70}")
    print(f"Date range:")
    print(f"  Start: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  End:   {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nHEALPix zoom level: {zoom}")
    if time_average:
        print(f"Temporal averaging: {time_average}")
    else:
        print("Temporal averaging: None (30-minute resolution)")
    print(f"\nOutput file: {output_file}")
    if overwrite:
        print("⚠️  Overwrite mode enabled - existing files will be replaced")
    
    # Create weights file path (use pathlib to handle extra slashes)
    weights_file = str(Path(config['weights_dir']) / f"ir_imerg_v07b_to_healpix_z{zoom}_weights.nc")
    
    # Get file search pattern configuration from config file
    date_pattern = config.get('date_pattern', r'\.(\d{8})-')  # Default to IMERG pattern if not specified
    date_format = config.get('date_format', '%Y%m%d')
    use_year_subdirs = config.get('use_year_subdirs', True)
    file_glob = config.get('file_glob', '*.nc*')
    
    print(f"\nFile search configuration:")
    print(f"  Pattern:          {date_pattern}")
    print(f"  Format:           {date_format}")
    print(f"  Year subdirs:     {use_year_subdirs}")
    print(f"  File glob:        {file_glob}")
    
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
        # File search pattern parameters
        date_pattern=date_pattern,
        date_format=date_format,
        use_year_subdirs=use_year_subdirs,
        file_glob=file_glob,
    )
    
    print(f"\nProcessing completed successfully!")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    main()
