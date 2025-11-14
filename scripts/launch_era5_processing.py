#!/usr/bin/env python3
"""
Launcher script for ERA5 to HEALPix processing

ERA5 data has unique organization:
- Monthly directories (YYYYMM)
- Daily files (24 hours per file)
- Each variable in separate files

Usage: 
    python launch_era5_processing.py START_DATE END_DATE [options]

Examples:
    python launch_era5_processing.py 2020-01-01 2020-01-31 -z 8
    python launch_era5_processing.py 2020-01-01 2020-12-31 -c custom_config.yaml -z 8 --overwrite
    python launch_era5_processing.py 2020-01-01 2020-01-07 -z 8 --vars T Q U V
"""

import sys
import yaml
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from remap_to_healpix import process_to_healpix_zarr
from src.utilities import parse_date, get_era5_input_files

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
  %(prog)s 2019-08-01 2020-09-01 -z 8
  %(prog)s 2019-08-01 2020-09-01 -z 8 --overwrite
  %(prog)s 2019-08-01 2020-09-01 -c custom_config.yaml -z 8
  %(prog)s 2019-08-01T00 2020-09-01T23 -z 8 -t 3h

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
                        help='Path to config YAML file (default: config/era5_config.yaml)')
    parser.add_argument('-z', '--zoom', type=int, default=None,
                        help='HEALPix zoom level (overrides config default)')
    parser.add_argument('-t', '--time-average', type=str, default=None,
                        help='Time averaging window (e.g., 1h, 3h, 6h, 1d) - overrides config')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing output files')
    parser.add_argument('--output', type=str, default=None,
                        help='Override output file path (default: auto-generated from config)')
    
    return parser.parse_args()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Process ERA5 data to HEALPix format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 2020-01-01 2020-01-31 -z 8
  %(prog)s 2020-01-01 2020-12-31 -z 8 --overwrite
  %(prog)s 2020-01-01 2020-01-07 -z 8 --vars T Q U V

Date formats:
  YYYY-MM-DD       - Date only (end_date extends to 23:59:59)
  YYYY-MM-DDTHH    - Date with hour (end_date extends to HH:59:59)
  YYYY-MM-DD HH    - Date with hour, space separated
  
Note: ERA5 data is organized with each variable in separate files.
      Use --vars to specify which variables to process (default: all in config).
        """
    )
    
    # Required positional arguments
    parser.add_argument('start_date', type=str,
                        help='Start date (YYYY-MM-DD or YYYY-MM-DDTHH)')
    parser.add_argument('end_date', type=str,
                        help='End date (YYYY-MM-DD or YYYY-MM-DDTHH)')
    
    # Optional arguments
    parser.add_argument('-c', '--config', type=str, default=None,
                        help='Path to config YAML file (default: config/era5_config.yaml)')
    parser.add_argument('-z', '--zoom', type=int, default=None,
                        help='HEALPix zoom level (overrides config default)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing output files')
    parser.add_argument('--vars', nargs='+',
                        help='Specific variables to process (e.g., T Q U V). Default: all in config')
    
    return parser.parse_args()

def main():
    """Main processing function"""
    args = parse_arguments()
    
    # Load configuration
    if args.config:
        config_path = Path(args.config)
    else:
        script_dir = Path(__file__).parent
        config_path = script_dir.parent / "config" / "era5_config.yaml"
    
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        print(f"Current directory: {Path.cwd()}")
        sys.exit(1)
    
    print(f"Loading configuration from: {config_path}")
    config = load_config(config_path)
    
    # Parse dates with flexible format support
    start_date = parse_date(args.start_date, is_end_date=False)
    end_date = parse_date(args.end_date, is_end_date=True)
    
    print(f"Processing ERA5 data from {start_date} to {end_date}")
    
    # Override zoom level if provided
    zoom = args.zoom if args.zoom is not None else config.get('default_zoom', 9)
    print(f"HEALPix zoom level: {zoom}")
    
    # Get ERA5 variable specifications
    all_variables = config.get('era5_variables', [])
    
    # Filter variables if --vars specified
    if args.vars:
        # User specified variable names (uppercase, like T, Q, U, V)
        requested_vars = [v.upper() for v in args.vars]
        variables = [v for v in all_variables if v['var_name'] in requested_vars]
        
        if len(variables) == 0:
            print(f"Error: None of the requested variables {args.vars} found in config")
            print(f"Available variables: {[v['var_name'] for v in all_variables]}")
            sys.exit(1)
        
        print(f"Processing selected variables: {[v['var_name'] for v in variables]}")
    else:
        variables = all_variables
        print(f"Processing all variables in config: {[v['var_name'] for v in variables]}")
    
    # Get input files for each variable using ERA5-specific function
    print(f"\nSearching for ERA5 files...")
    print(f"Base directory: {config['input_base_dir']}")
    
    file_dict = get_era5_input_files(
        start_date=start_date,
        end_date=end_date,
        base_dir=config['input_base_dir'],
        variables=variables,
        file_template=config.get('file_template', 
                                'e5.oper.an.pl.{var_code}.ll025{grid_type}.{date_start}_{date_end}.nc')
    )
    
    # Check if we found files
    total_files = sum(len(files) for files in file_dict.values())
    if total_files == 0:
        print("Error: No files found matching the criteria")
        sys.exit(1)
    
    print(f"\nFound {total_files} total files across {len(file_dict)} variables")
    for var_name, files in file_dict.items():
        print(f"  {var_name}: {len(files)} files")
    
    # Update weights file path with zoom level
    weights_file = config.get('weights_file', '').format(zoom=zoom)
    print(f"\nWeights file: {weights_file}")
    
    # Build output path
    output_dir = Path(config['output_base_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    output_zarr = output_dir / f"{config['output_basename']}_zoom{zoom}_{start_str}_{end_str}.zarr"
    
    print(f"Output: {output_zarr}")

    # Update config with input files
    additional_config = {
        'input_files': file_dict,  # Dict of variable: [files]
        'combine_vars': True,       # Merge variables into single dataset
    }
    config.update(additional_config)
    
    print("\n" + "="*80)
    print("Starting ERA5 to HEALPix processing...")
    print("="*80 + "\n")
    
    # Run processing
    process_to_healpix_zarr(
        start_date=start_date,
        end_date=end_date,
        zoom=zoom,
        output_zarr=str(output_zarr),
        weights_file=weights_file,
        overwrite=args.overwrite,
        config=config,
    )
    
    print("\n" + "="*80)
    print("ERA5 processing completed!")
    print("="*80)
    print(f"\nOutput saved to: {output_zarr}")

if __name__ == "__main__":
    main()
