#!/usr/bin/env python3
"""
Flexible launcher script for SCREAM to HEALPix processing

This script processes SCREAM data files with flexible file pattern matching.
It reads configuration from a YAML config file (default: scream_2d_config.yaml).

Usage: 
    python launch_scream_processing.py START_DATE END_DATE [options]

Examples:
    python launch_scream_processing.py 2019-08-01 2020-09-01 -z 9
    python launch_scream_processing.py 2019-08-01 2020-09-01 -z 9 --overwrite
    python launch_scream_processing.py 2019-08-01 2020-09-01 -c custom_config.yaml -z 9

Configuration:
    Edit config/scream_2d_config.yaml (or specify custom config) to configure:
    - Input/output paths
    - File search patterns (date_pattern, date_format, use_year_subdirs, file_glob)
    - Processing parameters (zoom level, time averaging, chunking)
    - Dask cluster settings
    - original_time_suffix: Time resolution of source data (e.g., '1H', '3H', '1D')
"""

import sys
import yaml
import argparse
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from remap_to_healpix import process_to_healpix_zarr
from src.utilities import parse_date

def load_config(config_path):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Process SCREAM data to HEALPix format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 2019-08-01 2020-09-01 -z 9
  %(prog)s 2019-08-01 2020-09-01 -z 9 --overwrite
  %(prog)s 2019-08-01 2020-09-01 -c custom_config.yaml -z 9
  %(prog)s 2019-08-01T00 2020-09-01T23 -z 9 -t 3h

Date formats:
  YYYY-MM-DD       - Date only (end_date extends to 23:59:59)
  YYYY-MM-DDTHH    - Date with hour (end_date extends to HH:59:59)
  YYYY-MM-DD HH    - Date with hour, space separated
        """
    )
    
    # Required positional arguments
    parser.add_argument('start_date', type=str,
                        help='Start date (YYYY-MM-DD or YYYY-MM-DDTHH)')
    parser.add_argument('end_date', type=str,
                        help='End date (YYYY-MM-DD or YYYY-MM-DDTHH)')
    
    # Optional arguments
    parser.add_argument('-c', '--config', type=str, default=None,
                        help='Path to config YAML file (default: config/scream_2d_config.yaml)')
    parser.add_argument('-z', '--zoom', type=int, default=None,
                        help='HEALPix zoom level (overrides config default)')
    parser.add_argument('-t', '--time-average', type=str, default=None,
                        help='Time averaging window (e.g., 1h, 3h, 6h, 1d) - overrides config')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing output files')
    parser.add_argument('--output', type=str, default=None,
                        help='Override output file path (default: auto-generated from config)')
    
    return parser.parse_args()

def main():
    # Parse arguments
    args = parse_arguments()
    
    # Parse dates
    start_date = parse_date(args.start_date, is_end_date=False)
    end_date = parse_date(args.end_date, is_end_date=True)
    
    # Get config path
    if args.config:
        config_path = Path(args.config)
    else:
        script_dir = Path(__file__).parent
        config_path = script_dir.parent / "config" / "scream_2d_config.yaml"
    
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    # Load configuration
    config = load_config(config_path=str(config_path))
    
    # Create output directory if it doesn't exist
    output_dir = Path(config['output_base_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Use command line arguments or config defaults
    zoom = args.zoom or config['default_zoom']
    time_average = args.time_average or config.get('time_average')
    overwrite = args.overwrite
    output_basename = config.get('output_basename', 'SCREAMv1')

    # Determine time suffix for filename
    # Priority: 1) time_average (if specified), 2) original_time_suffix from config (required)
    if time_average:
        # Convert time averaging to short format for filename
        time_suffix = time_average.upper().replace('H', 'H').replace('D', 'D')
    elif 'original_time_suffix' in config:
        # Use the original data time resolution from config
        time_suffix = config['original_time_suffix'].upper()
    else:
        # Error: original_time_suffix must be specified in config
        print("Error: 'original_time_suffix' must be specified in config file")
        print("Add this to your config YAML:")
        print("  original_time_suffix: '1H'  # or '3H', '1D', etc.")
        sys.exit(1)
    
    # Create output filename
    if args.output:
        output_file = args.output
    else:
        date_range = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        filename = f"{output_basename}_{time_suffix}_zoom{zoom}_{date_range}.zarr"
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
    
    # Require weights file path for efficient caching
    if 'weights_file' not in config or not config['weights_file']:
        print("\n" + "="*70)
        print("ERROR: weights_file must be specified in config")
        print("="*70)
        print("The weights file is required for efficient processing.")
        print("Add this to your config YAML:")
        print("")
        print("weights_file: \"/path/to/weights/scream_to_healpix_z{zoom}_weights.nc\"")
        print("")
        print("Note: The file will be created automatically on first run if it doesn't exist,")
        print("      then reused on subsequent runs for much faster processing.")
        print("="*70)
        sys.exit(1)
    
    weights_file = str(Path(config['weights_file']))
    if Path(weights_file).exists():
        print(f"Using existing weights file: {weights_file}")
    else:
        print(f"Weights file will be created: {weights_file}")
        print(f"  (Weights will be computed once and cached for future runs)")
    
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
    
    if config.get('remap_variables'):
        print(f"\nVariable remapping:   {len(config['remap_variables'])} mappings")
        for old, new in list(config['remap_variables'].items())[:3]:
            print(f"  {old} → {new}")
        if len(config['remap_variables']) > 3:
            print(f"  ... and {len(config['remap_variables']) - 3} more")
    
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
