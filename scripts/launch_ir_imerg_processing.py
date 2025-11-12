#!/usr/bin/env python3
"""
Flexible launcher script for IR+IMERG to HEALPix processing

This script processes IR+IMERG data files with flexible file pattern matching.
It reads configuration from a YAML config file (default: tb_imerg_config.yaml).

Usage: 
    python launch_ir_imerg_processing.py START_DATE END_DATE [options]

Examples:
    python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 -z 9
    python launch_ir_imerg_processing.py 2020-01-01 2020-01-31 -z 9 --overwrite
    python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 -c custom_config.yaml -z 9
    python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 -z 9 --time-subset 00min

Configuration:
    Edit config/tb_imerg_config.yaml (or specify custom config) to configure:
    - Input/output paths
    - File search patterns (date_pattern, date_format, use_year_subdirs, file_glob)
    - Processing parameters (zoom level, time averaging, chunking)
    - Dask cluster settings
    - original_time_suffix: Time resolution of source data (e.g., '30MIN', '1H', '1D')
"""

import sys
import yaml
import argparse
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from remap_to_healpix import process_to_healpix_zarr
from src.utilities import parse_date
from src.preprocessing import subset_time_by_minute

def load_config(config_path):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Process IR+IMERG data to HEALPix format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 2020-01-01 2020-12-31 -z 9
  %(prog)s 2020-01-01 2020-01-31 -z 9 --overwrite
  %(prog)s 2020-01-01 2020-12-31 -c custom_config.yaml -z 9
  %(prog)s 2020-01-01T00 2020-12-31T23 -z 9 -t 3h
  %(prog)s 2020-01-01 2020-12-31 -z 9 --time-subset 00min

Date formats:
  YYYY-MM-DD       - Date only (end_date extends to 23:59:59)
  YYYY-MM-DDTHH    - Date with hour (end_date extends to HH:59:59)
  YYYY-MM-DD HH    - Date with hour, space separated

Time subsetting (optional):
  --time-subset 00min  - Keep only times at ~00 minutes (e.g., 00:00, 01:00, 02:00)
  --time-subset 30min  - Keep only times at ~30 minutes (e.g., 00:30, 01:30, 02:30)
  (Useful when hourly-averaged data is stored at 30-min intervals - reduces output by 50%)
        """
    )
    
    # Required positional arguments
    parser.add_argument('start_date', type=str,
                        help='Start date (YYYY-MM-DD or YYYY-MM-DDTHH)')
    parser.add_argument('end_date', type=str,
                        help='End date (YYYY-MM-DD or YYYY-MM-DDTHH)')
    
    # Optional arguments
    parser.add_argument('-c', '--config', type=str, default=None,
                        help='Path to config YAML file (default: config/tb_imerg_config.yaml)')
    parser.add_argument('-z', '--zoom', type=int, default=None,
                        help='HEALPix zoom level (overrides config default)')
    parser.add_argument('-t', '--time-average', type=str, default=None,
                        help='Time averaging window (e.g., 1h, 3h, 6h, 1d) - overrides config')
    parser.add_argument('--time-subset', type=str, choices=['00min', '30min'], default=None,
                        help='Subset times by minute (00min or 30min)')
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
        config_path = script_dir.parent / "config" / "tb_imerg_config.yaml"
    
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
    time_subset = args.time_subset  # None, '00min', or '30min'
    output_basename = config.get('output_basename', 'IR_IMERG_V7')

    # Determine time suffix for filename
    # Priority: 1) time_average (if specified), 2) original_time_suffix from config, 3) default to '30MIN'
    if time_average:
        # Convert time averaging to short format for filename
        time_suffix = time_average.upper().replace('H', 'H').replace('D', 'D')
    elif 'original_time_suffix' in config:
        # Use the original data time resolution from config
        time_suffix = config['original_time_suffix'].upper()
    else:
        # Default to 30MIN (half-hourly)
        time_suffix = '30MIN'
    
    # Create output filename
    if args.output:
        output_file = args.output
    else:
        date_range = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        
        # Add time subset to filename if specified
        # subset_suffix = f"_{time_subset}" if time_subset else ""
        # filename = f"{output_basename}_{time_suffix}{subset_suffix}_zoom{zoom}_{date_range}.zarr"
        filename = f"{output_basename}_{time_suffix}_zoom{zoom}_{date_range}.zarr"
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
        print(f"Temporal averaging: None ({time_suffix} resolution)")
    if time_subset:
        print(f"Time subsetting: {time_subset}")
    print(f"\nOutput file: {output_file}")
    if overwrite:
        print("⚠️  Overwrite mode enabled - existing files will be replaced")
    
    # Determine weights file path
    # Priority: 1) weights_file in config (explicit), 2) weights_dir + auto-generated name (backward compat)
    if 'weights_file' in config and config['weights_file']:
        # Use explicitly specified weights file from config
        weights_file = str(Path(config['weights_file']))
        print(f"Using explicit weights file from config: {weights_file}")
    elif 'weights_dir' in config:
        # Backward compatibility: auto-generate weights filename from weights_dir
        weights_file = str(Path(config['weights_dir']) / f"ir_imerg_v07b_to_healpix_z{zoom}_weights.nc")
        print(f"Auto-generated weights file from weights_dir: {weights_file}")
    else:
        # No weights file specified - will compute on-the-fly (slower)
        weights_file = None
        print("⚠️  No weights_file or weights_dir specified - weights will be computed on-the-fly (slower)")
    
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
    
    if time_subset:
        print(f"\n⏰ Time subsetting: {time_subset} (output will be reduced by ~50%)")
    
    # Get spatial dimensions from config (optional - will auto-detect if not specified)
    spatial_dimensions = config.get('spatial_dimensions', None)
    if spatial_dimensions:
        print(f"\n📐 Spatial dimensions (from config): {spatial_dimensions}")
    else:
        print(f"\n📐 Spatial dimensions: Will auto-detect from data files")
    
    # Get time dimension name from config (optional - defaults to 'time')
    concat_dim = config.get('concat_dim', 'time')
    if concat_dim != 'time':
        print(f"\n⏱️  Time dimension name (from config): '{concat_dim}'")
    
    # Prepare preprocessing function and kwargs for new flexible approach
    preprocessing_func = None
    preprocessing_kwargs = None
    if time_subset:
        preprocessing_func = subset_time_by_minute
        preprocessing_kwargs = {'time_subset': time_subset}
        print(f"✨ Using flexible preprocessing: subset_time_by_minute with time_subset='{time_subset}'")
    
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
