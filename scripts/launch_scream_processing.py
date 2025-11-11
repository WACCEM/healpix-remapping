#!/usr/bin/env python3
"""
Simple launcher script for SCREAM to HEALPix processing

This script processes SCREAM data files with flexible file pattern matching.
It reads configuration from scream_2d_config.yaml including file search patterns.

Usage: 
    python launch_scream_processing.py <start_date> <end_date> [zoom_level] [--overwrite]

Examples:
    python launch_scream_processing.py 2020-01-01 2020-12-31 9
    python launch_scream_processing.py 2020-01-01 2020-01-31 9 --overwrite

Configuration:
    Edit config/scream_2d_config.yaml to configure:
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

from remap_to_healpix import process_to_healpix_zarr
from src.utilities import parse_date

def load_config(config_path):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def main():
    if len(sys.argv) < 3:
        print("Usage: python launch_scream_processing.py <start_date> <end_date> [zoom_level] [--overwrite] [--time-subset=00min|30min]")
        print("\nDate formats:")
        print("  YYYY-MM-DD       - Date only (end_date extends to 23:59:59)")
        print("  YYYY-MM-DDTHH    - Date with hour (end_date extends to HH:59:59)")
        print("  YYYY-MM-DD HH    - Date with hour, space separated")
        # print("\nTime subsetting (optional):")
        # print("  --time-subset=00min  - Keep only times at ~00 minutes (e.g., 00:00, 01:00, 02:00)")
        # print("  --time-subset=30min  - Keep only times at ~30 minutes (e.g., 00:30, 01:30, 02:30)")
        # print("  (Useful when hourly-averaged data is stored at 30-min intervals - reduces output by 50%)")
        print("\nExamples:")
        print("  python launch_scream_processing.py 2020-01-01 2020-12-31 9")
        print("  python launch_scream_processing.py 2020-01-01T00 2020-01-03T23 9")
        print("  python launch_scream_processing.py '2020-01-01 00' '2020-01-03 23' 9")
        print("  python launch_scream_processing.py 2020-01-01 2020-12-31 9 --overwrite")
        # print("  python launch_scream_processing.py 2020-01-01 2020-12-31 9 --time-subset=00min")
        sys.exit(1)
    
    # Parse command line arguments
    start_date = parse_date(sys.argv[1], is_end_date=False)
    end_date = parse_date(sys.argv[2], is_end_date=True)
    
    # Get config path relative to script location
    script_dir = Path(__file__).parent
    config_path = script_dir.parent / "config" / "scream_2d_config.yaml"
    
    # Parse zoom level and options
    zoom = None
    overwrite = True
    # time_subset = '00min'  # Default to 00min subsetting
    
    # Process remaining arguments
    for arg in sys.argv[3:]:
        if arg == '--overwrite':
            overwrite = True
        # elif arg.startswith('--time-subset'):
        #     if '=' in arg:
        #         time_subset = arg.split('=')[1]
        #     else:
        #         time_subset = '00min'  # Default to 00min when flag is used without value
        #     if time_subset not in ['00min', '30min']:
        #         print(f"Error: Invalid time-subset value '{time_subset}'. Must be '00min' or '30min'")
        #         sys.exit(1)
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
    output_basename = config.get('output_basename', 'SCREAMv1')

    # Create output filename with time averaging info
    date_range = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    # Default to 1H, update if time_average is specified
    if time_average:
        # Convert time averaging to short format for filename
        time_suffix = time_average.upper().replace('H', 'H').replace('D', 'D')
    else:
        time_suffix = '1H'  # Default to 1H (hourly)
    
    filename = f"{output_basename}_{time_suffix}_zoom{zoom}_{date_range}.zarr"
    
    # Use pathlib to properly construct the path (handles extra slashes automatically)
    output_file = str(Path(config['output_base_dir']) / filename)

    print(f"\n{'='*70}")
    print(f"Processing SCREAM data")
    print(f"{'='*70}")
    print(f"Date range:")
    print(f"  Start: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  End:   {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nHEALPix zoom level: {zoom}")
    if time_average:
        print(f"Temporal averaging: {time_average}")
    else:
        print("Temporal averaging: None (original resolution)")
    print(f"\nOutput file: {output_file}")
    if overwrite:
        print("⚠️  Overwrite mode enabled - existing files will be replaced")
    
    # Create weights file path (use pathlib to handle extra slashes)
    weights_file = str(Path(config['weights_dir']) / f"SCREAM_ne1024pg2_to_healpix_z{zoom}_weights.nc")
    
    # Display configuration summary
    print(f"\n{'='*70}")
    print("Configuration Summary")
    print(f"{'='*70}")
    print(f"Input directory:      {config['input_base_dir']}")
    print(f"Time chunk size:      {config.get('time_chunk_size', 48)}")
    print(f"Grid type:            {config.get('grid_type', 'auto')}")
    
    if config.get('spatial_dimensions'):
        print(f"Spatial dimensions:   {config['spatial_dimensions']}")
    else:
        print(f"Spatial dimensions:   Auto-detect from data files")
    
    print(f"\nFile search:")
    print(f"  Pattern:            {config.get('date_pattern', 'default')}")
    print(f"  Format:             {config.get('date_format', 'default')}")
    print(f"  Year subdirs:       {config.get('use_year_subdirs', True)}")
    print(f"  File glob:          {config.get('file_glob', '*.nc*')}")
    
    if config.get('skip_variables'):
        print(f"\nVariable filtering:")
        print(f"  Skip variables:     {len(config['skip_variables'])} patterns")
        print(f"  Required dims:      {config.get('required_dimensions', 'None')}")
    
    if config.get('var_rename_map'):
        print(f"\nVariable renaming:    {len(config['var_rename_map'])} mappings")
        for old, new in list(config['var_rename_map'].items())[:3]:
            print(f"  {old} → {new}")
        if len(config['var_rename_map']) > 3:
            print(f"  ... and {len(config['var_rename_map']) - 3} more")
    
    print(f"{'='*70}")
    
    # Prepare preprocessing function and kwargs for new flexible approach
    preprocessing_func = None
    preprocessing_kwargs = None
    
    # Run the processing with config dictionary
    process_to_healpix_zarr(
        start_date=start_date,
        end_date=end_date,
        zoom=zoom,
        output_zarr=output_file,
        weights_file=weights_file,
        overwrite=overwrite,
        time_average=time_average,
        preprocessing_func=preprocessing_func,
        preprocessing_kwargs=preprocessing_kwargs,
        config=config,  # Pass entire config dictionary
    )
    
    print(f"\n✅ Processing completed successfully!")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    main()
