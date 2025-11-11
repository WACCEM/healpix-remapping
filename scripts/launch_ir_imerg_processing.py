#!/usr/bin/env python3
"""
Simple launcher script for IR+IMERG to HEALPix processing

This script processes IR+IMERG data files with flexible file pattern matching.
It reads configuration from tb_imerg_config.yaml including file search patterns.

Usage: 
    python launch_ir_imerg_processing.py <start_date> <end_date> [zoom_level] [--overwrite] [--time-subset=00min|30min]

Examples:
    python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9
    python launch_ir_imerg_processing.py 2020-01-01 2020-01-31 9 --overwrite
    python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9 --time-subset=00min

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

from remap_to_healpix import process_to_healpix_zarr
from src.utilities import parse_date
from src.preprocessing import subset_time_by_minute

def load_config(config_path="tb_imerg_config.yaml"):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def main():
    if len(sys.argv) < 3:
        print("Usage: python launch_ir_imerg_processing.py <start_date> <end_date> [zoom_level] [--overwrite] [--time-subset=00min|30min]")
        print("\nDate formats:")
        print("  YYYY-MM-DD       - Date only (end_date extends to 23:59:59)")
        print("  YYYY-MM-DDTHH    - Date with hour (end_date extends to HH:59:59)")
        print("  YYYY-MM-DD HH    - Date with hour, space separated")
        print("\nTime subsetting (optional):")
        print("  --time-subset=00min  - Keep only times at ~00 minutes (e.g., 00:00, 01:00, 02:00)")
        print("  --time-subset=30min  - Keep only times at ~30 minutes (e.g., 00:30, 01:30, 02:30)")
        print("  (Useful when hourly-averaged data is stored at 30-min intervals - reduces output by 50%)")
        print("\nExamples:")
        print("  python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9")
        print("  python launch_ir_imerg_processing.py 2020-01-01T00 2020-01-03T23 9")
        print("  python launch_ir_imerg_processing.py '2020-01-01 00' '2020-01-03 23' 9")
        print("  python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9 --overwrite")
        print("  python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9 --time-subset=00min")
        sys.exit(1)
    
    # Parse command line arguments
    start_date = parse_date(sys.argv[1], is_end_date=False)
    end_date = parse_date(sys.argv[2], is_end_date=True)
    
    # Get config path relative to script location
    script_dir = Path(__file__).parent
    config_path = script_dir.parent / "config" / "tb_imerg_config.yaml"
    
    # Parse zoom level and options
    zoom = None
    overwrite = True
    time_subset = '00min'  # Default to 00min subsetting
    
    # Process remaining arguments
    for arg in sys.argv[3:]:
        if arg == '--overwrite':
            overwrite = True
        elif arg.startswith('--time-subset'):
            if '=' in arg:
                time_subset = arg.split('=')[1]
            else:
                time_subset = '00min'  # Default to 00min when flag is used without value
            if time_subset not in ['00min', '30min']:
                print(f"Error: Invalid time-subset value '{time_subset}'. Must be '00min' or '30min'")
                sys.exit(1)
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
    output_basename = config.get('output_basename', 'IR_IMERG_V7')

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
