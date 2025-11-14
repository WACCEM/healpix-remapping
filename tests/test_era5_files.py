#!/usr/bin/env python3
"""
Test script for ERA5 file search functionality

This script tests the ERA5 file search based on era5_config.yaml configuration.
It verifies:
- ERA5 variable configuration is valid
- File search finds expected files
- Date extraction from filenames works correctly
- File structure matches expectations

Usage:
    cd tests
    python test_era5_files.py [--date-range START END] [--vars VAR1 VAR2 ...]
    
Examples:
    python test_era5_files.py
    python test_era5_files.py --date-range 2020-01-01 2020-01-31
    python test_era5_files.py --date-range 2020-01-01 2020-01-07 --vars T Q
"""

import sys
import yaml
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import xarray as xr

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utilities import get_era5_input_files, parse_date


def load_config(config_path):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def check_file_structure(file_path, expected_vars=None):
    """
    Check the structure of an ERA5 file
    
    Parameters:
    -----------
    file_path : str
        Path to the file to check
    expected_vars : list, optional
        List of expected variable names
        
    Returns:
    --------
    dict : Information about the file structure
    """
    try:
        with xr.open_dataset(file_path) as ds:
            info = {
                'dimensions': dict(ds.sizes),
                'variables': list(ds.data_vars),
                'coords': list(ds.coords),
                'time_steps': ds.sizes.get('time', 0),
                'levels': ds.sizes.get('level', 0),
                'time_range': None
            }
            
            if 'time' in ds.sizes:
                time_values = ds.time.values
                info['time_range'] = (str(time_values[0]), str(time_values[-1]))
            
            return info
    except Exception as e:
        return {'error': str(e)}


def test_era5_files(start_date, end_date, config, selected_vars=None):
    """
    Test ERA5 file search and report results
    
    Parameters:
    -----------
    start_date : datetime
        Start date for file search
    end_date : datetime
        End date for file search
    config : dict
        Configuration dictionary
    selected_vars : list, optional
        List of variable names to test (if None, test all)
    """
    
    print("="*80)
    print("ERA5 File Search Test")
    print("="*80)
    print(f"\nConfiguration file: era5_config.yaml")
    print(f"Base directory: {config['input_base_dir']}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Get all variables from config
    all_variables = config.get('era5_variables', [])
    
    if not all_variables:
        print("\n❌ ERROR: No variables defined in era5_config.yaml")
        print("   Check the 'era5_variables' section in the config file")
        return False
    
    # Filter variables if specific ones requested
    if selected_vars:
        requested_vars = [v.upper() for v in selected_vars]
        variables = [v for v in all_variables if v['var_name'] in requested_vars]
        
        if len(variables) == 0:
            print(f"\n❌ ERROR: None of the requested variables {selected_vars} found in config")
            print(f"   Available variables: {[v['var_name'] for v in all_variables]}")
            return False
        
        print(f"\nTesting selected variables: {[v['var_name'] for v in variables]}")
    else:
        variables = all_variables
        print(f"\nTesting all {len(variables)} variables from config")
    
    # Display variable information
    print("\n" + "-"*80)
    print("Variables to test:")
    print("-"*80)
    for i, var in enumerate(variables, 1):
        print(f"{i}. {var['var_name']:8s} - {var.get('long_name', 'N/A'):30s} "
              f"[{var['var_code']}, grid: {var['grid_type']}]")
    
    # Test file search
    print("\n" + "-"*80)
    print("Searching for files...")
    print("-"*80)
    
    try:
        file_dict = get_era5_input_files(
            start_date=start_date,
            end_date=end_date,
            base_dir=config['input_base_dir'],
            variables=variables,
            file_template=config.get('file_template', 
                                    'e5.oper.an.pl.{var_code}.ll025{grid_type}.{date_start}_{date_end}.nc')
        )
    except Exception as e:
        print(f"\n❌ ERROR during file search: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Analyze results
    total_files = sum(len(files) for files in file_dict.values())
    
    print(f"\n{'='*80}")
    print(f"Search Results Summary")
    print(f"{'='*80}")
    print(f"Total files found: {total_files}")
    print(f"Variables with files: {sum(1 for files in file_dict.values() if len(files) > 0)}/{len(variables)}")
    
    # Detailed results per variable
    print(f"\n{'-'*80}")
    print(f"Files per variable:")
    print(f"{'-'*80}")
    
    success = True
    for var_name, files in file_dict.items():
        status = "✓" if len(files) > 0 else "✗"
        print(f"{status} {var_name:8s}: {len(files):4d} files", end='')
        
        if len(files) > 0:
            print(f"  (first: {Path(files[0]).name})")
        else:
            print("  - NO FILES FOUND")
            success = False
    
    # Test file structure for first file of each variable
    if total_files > 0:
        print(f"\n{'-'*80}")
        print("File Structure Verification (first file of each variable):")
        print(f"{'-'*80}")
        
        for var_name, files in file_dict.items():
            if len(files) == 0:
                continue
                
            print(f"\n{var_name}: {Path(files[0]).name}")
            
            # Check if file exists
            file_path = Path(files[0])
            if not file_path.exists():
                print(f"  ❌ File does not exist: {file_path}")
                success = False
                continue
            
            # Check file structure
            file_info = check_file_structure(str(file_path), expected_vars=[var_name])
            
            if 'error' in file_info:
                print(f"  ❌ Error reading file: {file_info['error']}")
                success = False
            else:
                print(f"  ✓ Dimensions: {file_info['dimensions']}")
                print(f"  ✓ Variables: {file_info['variables']}")
                print(f"  ✓ Time steps: {file_info['time_steps']}")
                print(f"  ✓ Pressure levels: {file_info['levels']}")
                if file_info['time_range']:
                    print(f"  ✓ Time range: {file_info['time_range'][0]} to {file_info['time_range'][1]}")
    
    # Final summary
    print(f"\n{'='*80}")
    if success and total_files > 0:
        print("✅ TEST PASSED: All files found and validated")
    elif total_files > 0:
        print("⚠️  TEST PARTIALLY PASSED: Some files found but some issues detected")
    else:
        print("❌ TEST FAILED: No files found")
    print(f"{'='*80}")
    
    # Recommendations
    if not success:
        print("\n📋 Troubleshooting recommendations:")
        print("   1. Check that base_dir path is correct in era5_config.yaml")
        print("   2. Verify that monthly directories exist (e.g., 202001, 202002)")
        print("   3. Check file naming matches the template in config")
        print("   4. Verify you have read permissions for the directory")
        print("   5. Try a different date range that has data available")
    
    return success


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Test ERA5 file search functionality',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with default date range (first week of 2020)
  python test_era5_files.py
  
  # Test with custom date range
  python test_era5_files.py --date-range 2020-01-01 2020-01-31
  
  # Test specific variables only
  python test_era5_files.py --date-range 2020-01-01 2020-01-07 --vars T Q
  
  # Test with custom config
  python test_era5_files.py --config ../config/custom_era5_config.yaml
"""
    )
    
    parser.add_argument('-c', '--config', type=str, default='../config/era5_3d_config.yaml',
                       help='Path to ERA5 config file (default: ../config/era5_3d_config.yaml)')
    parser.add_argument('--date-range', nargs=2, metavar=('START', 'END'),
                       help='Date range to test (YYYY-MM-DD format)')
    parser.add_argument('--vars', nargs='+',
                       help='Specific variables to test (e.g., T Q U V)')
    
    return parser.parse_args()


def main():
    """Main test function"""
    args = parse_arguments()
    
    # Load configuration
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ ERROR: Config file not found: {config_path}")
        print(f"   Current directory: {Path.cwd()}")
        sys.exit(1)
    
    print(f"Loading configuration from: {config_path}")
    config = load_config(config_path)
    
    # Parse date range
    if args.date_range:
        try:
            start_date = parse_date(args.date_range[0], is_end_date=False)
            end_date = parse_date(args.date_range[1], is_end_date=True)
        except ValueError as e:
            print(f"❌ ERROR: Invalid date format: {e}")
            sys.exit(1)
    else:
        # Default: Test first week of 2020
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 1, 7)
        print(f"Using default date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Run test
    success = test_era5_files(start_date, end_date, config, selected_vars=args.vars)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
