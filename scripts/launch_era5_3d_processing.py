#!/usr/bin/env python
"""
Launch ERA5 3D pressure level variable processing to HEALPix grid.

This script handles ERA5 3D pressure level variables which are stored in daily files
within monthly directories (different from 2D surface variables).

Example usage:
    # Process all 3D variables for January 2020
    python scripts/launch_era5_3d_processing.py 2020-01-01 2020-01-31 -z 8
    
    # Process specific variables
    python scripts/launch_era5_3d_processing.py 2020-01-01 2020-01-31 -z 8 \
        --vars T Q U V
    
    # Process with custom output directory
    python scripts/launch_era5_3d_processing.py 2020-01-01 2020-01-31 -z 8 \
        --output-dir /path/to/output
    
    # Time subsetting: Edit time_subset in config file (e.g., time_subset: '3h')
"""

import os
import sys
import argparse
import yaml
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from remap_to_healpix import process_to_healpix_zarr
from src.utilities import get_era5_input_files
from src.preprocessing import subset_time_by_interval, subset_vertical_levels

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Process ERA5 3D pressure level variables to HEALPix grid',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all 3D variables for January 2020 at zoom level 8
  %(prog)s 2020-01-01 2020-01-31 -z 8
  
  # Process specific variables
  %(prog)s 2020-01-01 2020-01-31 -z 8 --vars T Q U V
  
  # Time subsetting: Edit time_subset in config file (e.g., time_subset: '3h')
        """
    )
    
    # Required arguments
    parser.add_argument('start_date', type=str,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', type=str,
                       help='End date (YYYY-MM-DD)')
    
    # Optional arguments
    parser.add_argument('-z', '--zoom', type=int, default=8,
                       help='HEALPix zoom level (default: 8)')
    parser.add_argument('-c', '--config', type=str,
                       default='../config/era5_3d_config.yaml',
                       help='Config file path (default: config/era5_3d_config.yaml)')
    parser.add_argument('--vars', nargs='+', type=str,
                       help='Variables to process (default: all variables in config). '
                            'Use ERA5 variable names (e.g., T, Q, U, V)')
    parser.add_argument('--output-dir', type=str,
                       help='Override output directory from config')
    parser.add_argument('--output-name', type=str,
                       help='Override output filename (without .zarr extension)')
    parser.add_argument('--overwrite', action='store_true',
                       help='Overwrite output file if it exists')
    
    return parser.parse_args()


def load_config(config_path):
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def filter_variables(config, requested_vars=None):
    """
    Filter variables from config based on requested variable names.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary containing era5_variables list
    requested_vars : list of str, optional
        List of variable names to process. If None, use all variables.
        
    Returns
    -------
    list
        Filtered list of variable dictionaries
    """
    all_vars = config['era5_variables']
    
    if requested_vars is None:
        return all_vars
    
    # Create mapping of var_name to full variable dict
    var_map = {var['var_name']: var for var in all_vars}
    
    # Filter requested variables
    filtered_vars = []
    for var_name in requested_vars:
        if var_name in var_map:
            filtered_vars.append(var_map[var_name])
        else:
            print(f"Warning: Variable '{var_name}' not found in config. Skipping.")
            print(f"Available variables: {', '.join(var_map.keys())}")
    
    if not filtered_vars:
        raise ValueError("No valid variables selected for processing!")
    
    return filtered_vars


def main():
    """Main processing function."""
    args = parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    print(f"ERA5 3D Pressure Level Variable Processing")
    print(f"==========================================")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"HEALPix zoom level: {args.zoom}")
    print()
    
    # Load configuration
    config = load_config(args.config)
    print(f"Loaded config from: {args.config}")
    
    # Filter variables if requested
    variables = filter_variables(config, args.vars)
    print(f"Processing {len(variables)} variables:")
    for var in variables:
        print(f"  - {var['var_name']}")
    print()
    
    # Search for input files
    print("Searching for input files...")
    base_dir = config['input_base_dir']
    file_template = config['file_template']
    
    # Search for files (monthly_files=False for 3D pressure level variables)
    input_files = get_era5_input_files(
        start_date=start_date,
        end_date=end_date,
        base_dir=base_dir,
        variables=variables,
        file_template=file_template,
        monthly_files=False  # Key difference from 2D processing
    )
    
    # Print summary
    total_files = sum(len(files) for files in input_files.values())
    print(f"Found {total_files} files across {len(input_files)} variables:")
    for var_name, files in input_files.items():
        print(f"  {var_name}: {len(files)} files")
    print()
    
    if total_files == 0:
        print("ERROR: No input files found!")
        return 1
    
    # Update weights file path with zoom level
    weights_file = config.get('weights_file', '').format(zoom=args.zoom)
    print(f"Weights file: {weights_file}")
    
    # Build output path
    output_dir = Path(config['output_base_dir'])
    if args.output_dir:
        output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine time resolution suffix for output filename
    time_subset = config.get('time_subset', None)
    if time_subset:
        # Use time_subset (uppercase) if specified in config
        time_suffix = time_subset.upper()
    else:
        # Use original_time_suffix from config
        time_suffix = config.get('original_time_suffix', '1H').upper()
    
    date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    if args.output_name:
        output_filename = args.output_name
    else:
        output_filename = f"{config['output_basename']}_{time_suffix}_zoom{args.zoom}_{date_str}"
    
    output_zarr = output_dir / f"{output_filename}.zarr"
    print(f"Output: {output_zarr}")

    # Setup preprocessing functions
    preprocessing_func = []
    preprocessing_kwargs = []
    
    # Add vertical level subsetting if specified
    level_subset = config.get('level_subset', None)
    if level_subset:
        z_coordname = config.get('z_coordname', 'level')
        preprocessing_func.append(subset_vertical_levels)
        preprocessing_kwargs.append({
            'level_subset': level_subset,
            'z_coordname': z_coordname
        })
        print(f"📊 Vertical level subsetting: {len(level_subset)} levels selected")
        print(f"   Levels: {level_subset}")
    
    # Add time subsetting if specified
    if time_subset:
        preprocessing_func.append(subset_time_by_interval)
        preprocessing_kwargs.append({'time_subset': time_subset})
        print(f"⏰ Time subsetting: {time_subset} intervals")
    
    # Convert to single function/kwargs or None if empty
    if not preprocessing_func:
        preprocessing_func = None
        preprocessing_kwargs = None
    elif len(preprocessing_func) == 1:
        preprocessing_func = preprocessing_func[0]
        preprocessing_kwargs = preprocessing_kwargs[0]
    
    # Update config with input files
    additional_config = {
        'input_files': input_files,  # Dict of variable: [files]
        'combine_vars': True,         # Merge variables into single dataset
    }
    config.update(additional_config)
    
    print("\n" + "="*80)
    print("Starting ERA5 3D to HEALPix processing...")
    print("="*80 + "\n")
    
    # Run processing (same pattern as 2D launcher)
    try:
        process_to_healpix_zarr(
            start_date=start_date,
            end_date=end_date,
            zoom=args.zoom,
            output_zarr=str(output_zarr),
            weights_file=weights_file,
            overwrite=args.overwrite,
            preprocessing_func=preprocessing_func,
            preprocessing_kwargs=preprocessing_kwargs,
            config=config,
        )
        
        print("\n" + "="*80)
        print("✅ ERA5 3D processing completed!")
        print("="*80)
        print(f"\nOutput saved to: {output_zarr}")
        return 0
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"❌ ERROR during processing: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
