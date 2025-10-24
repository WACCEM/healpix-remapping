#!/usr/bin/env python3
"""
Quick test to verify IR_IMERG file pattern matching works correctly.
"""

import re
from datetime import datetime
from pathlib import Path
import yaml

# Load config
config_path = Path(__file__).parent / "config" / "tb_imerg_config.yaml"
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# Test filename
test_filename = "merg_2020010100_10km-pixel.nc"

print("="*70)
print("Testing IR_IMERG File Pattern Configuration")
print("="*70)
print(f"\nTest filename: {test_filename}")
print(f"\nConfiguration:")
print(f"  date_pattern:     {config['date_pattern']}")
print(f"  date_format:      {config['date_format']}")
print(f"  use_year_subdirs: {config['use_year_subdirs']}")
print(f"  file_glob:        {config['file_glob']}")

# Test extraction
date_regex = re.compile(config['date_pattern'])
match = date_regex.search(test_filename)

if match:
    date_str = match.group(1)
    print(f"\n✓ Pattern matched!")
    print(f"  Extracted string: '{date_str}'")
    
    # Test parsing
    try:
        file_date = datetime.strptime(date_str, config['date_format'])
        print(f"\n✓ Date parsed successfully!")
        print(f"  Parsed datetime: {file_date}")
        print(f"  Year: {file_date.year}, Month: {file_date.month}, Day: {file_date.day}, Hour: {file_date.hour}")
        print(f"\n✅ SUCCESS: Pattern configuration is correct!")
    except ValueError as e:
        print(f"\n❌ FAILED: Could not parse date")
        print(f"   Error: {e}")
else:
    print(f"\n❌ FAILED: Pattern did not match filename")

# Test with actual directory (if accessible)
print("\n" + "="*70)
print("Testing with actual files...")
print("="*70)

base_dir = Path(config['input_base_dir'])
if base_dir.exists():
    # Check 2020 directory
    year_dir = base_dir / "2020"
    if year_dir.exists():
        files = sorted(year_dir.glob(config['file_glob']))
        if files:
            print(f"\n✓ Found {len(files)} files in {year_dir}")
            print(f"\nTesting first 5 files:")
            
            success_count = 0
            for i, file_path in enumerate(files[:5]):
                filename = file_path.name
                match = date_regex.search(filename)
                if match:
                    date_str = match.group(1)
                    try:
                        file_date = datetime.strptime(date_str, config['date_format'])
                        print(f"  {i+1}. {filename}")
                        print(f"     → {file_date}")
                        success_count += 1
                    except:
                        print(f"  {i+1}. {filename} ✗ Parse failed")
                else:
                    print(f"  {i+1}. {filename} ✗ No match")
            
            if success_count == 5:
                print(f"\n✅ All test files processed successfully!")
            else:
                print(f"\n⚠️  {5 - success_count} files failed")
        else:
            print(f"No files matching '{config['file_glob']}' found in {year_dir}")
    else:
        print(f"Directory not found: {year_dir}")
else:
    print(f"Base directory not accessible: {base_dir}")
    print("(This is normal if running from a login node without scratch access)")

print("\n" + "="*70)
print("Configuration is ready for use with launch_ir_imerg_processing.py")
print("="*70)
