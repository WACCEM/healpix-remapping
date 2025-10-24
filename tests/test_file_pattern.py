#!/usr/bin/env python3
"""
Test script to verify file pattern matching before running the full pipeline.

This helps debug date_pattern and date_format configuration.
"""

import re
from datetime import datetime
from pathlib import Path
import glob


def test_file_pattern(test_filename, date_pattern, date_format):
    """
    Test if a date pattern correctly extracts and parses a date from a filename.
    
    Parameters:
    -----------
    test_filename : str
        Example filename to test
    date_pattern : str
        Regex pattern to extract date string
    date_format : str
        strptime format to parse the date
    
    Returns:
    --------
    bool : True if successful, False otherwise
    """
    print(f"\n{'='*70}")
    print(f"Testing filename: {test_filename}")
    print(f"Date pattern:     {date_pattern}")
    print(f"Date format:      {date_format}")
    print(f"{'='*70}")
    
    try:
        # Compile regex
        date_regex = re.compile(date_pattern)
        
        # Try to match
        match = date_regex.search(test_filename)
        
        if not match:
            print("❌ FAILED: Pattern did not match filename")
            print(f"   No match found in '{test_filename}'")
            return False
        
        # Extract captured group
        date_str = match.group(1)
        print(f"✓ Pattern matched!")
        print(f"  Extracted string: '{date_str}'")
        
        # Try to parse
        file_date = datetime.strptime(date_str, date_format)
        print(f"✓ Date parsed successfully!")
        print(f"  Parsed datetime: {file_date}")
        print(f"  Year: {file_date.year}, Month: {file_date.month}, Day: {file_date.day}", end="")
        if file_date.hour != 0 or file_date.minute != 0:
            print(f", Hour: {file_date.hour}, Minute: {file_date.minute}")
        else:
            print()
        
        print(f"\n✅ SUCCESS: Pattern works correctly!")
        return True
        
    except re.error as e:
        print(f"❌ FAILED: Invalid regex pattern")
        print(f"   Error: {e}")
        return False
        
    except ValueError as e:
        print(f"❌ FAILED: Could not parse date string '{date_str}'")
        print(f"   Error: {e}")
        print(f"   Hint: Check that date_format ('{date_format}') matches the extracted string")
        return False
        
    except Exception as e:
        print(f"❌ FAILED: Unexpected error")
        print(f"   Error: {e}")
        return False


def test_directory_scan(base_dir, date_pattern, date_format, 
                       use_year_subdirs=True, file_glob='*.nc*',
                       start_date=None, end_date=None, max_display=10):
    """
    Test scanning a directory for files and parsing their dates.
    
    Parameters:
    -----------
    base_dir : str
        Base directory to scan
    date_pattern : str
        Regex pattern to extract date
    date_format : str
        strptime format to parse date
    use_year_subdirs : bool
        Whether to look in yearly subdirectories
    file_glob : str
        Glob pattern for files
    start_date : datetime, optional
        Filter by start date
    end_date : datetime, optional
        Filter by end date
    max_display : int
        Maximum number of files to display details for
    """
    print(f"\n{'='*70}")
    print(f"Testing directory scan")
    print(f"{'='*70}")
    print(f"Base directory:   {base_dir}")
    print(f"Use year subdirs: {use_year_subdirs}")
    print(f"File glob:        {file_glob}")
    print(f"Date pattern:     {date_pattern}")
    print(f"Date format:      {date_format}")
    if start_date:
        print(f"Start date:       {start_date}")
    if end_date:
        print(f"End date:         {end_date}")
    print(f"{'='*70}\n")
    
    base_path = Path(base_dir)
    
    if not base_path.exists():
        print(f"❌ Directory does not exist: {base_dir}")
        return
    
    # Get file list
    if use_year_subdirs:
        if start_date and end_date:
            years = range(start_date.year, end_date.year + 1)
        else:
            # Scan all year subdirectories
            years = [int(d.name) for d in base_path.iterdir() 
                    if d.is_dir() and d.name.isdigit()]
        
        all_files = []
        for year in years:
            year_dir = base_path / str(year)
            if year_dir.exists():
                year_files = sorted(glob.glob(str(year_dir / file_glob)))
                all_files.extend(year_files)
                print(f"Year {year}: found {len(year_files)} files")
    else:
        all_files = sorted(glob.glob(str(base_path / file_glob)))
        print(f"Found {len(all_files)} files in {base_dir}")
    
    if not all_files:
        print(f"\n❌ No files found matching pattern '{file_glob}'")
        return
    
    print(f"\nTotal files found: {len(all_files)}")
    print(f"\nTesting date extraction on first {min(max_display, len(all_files))} files:")
    print(f"{'-'*70}")
    
    # Test date extraction
    date_regex = re.compile(date_pattern)
    success_count = 0
    fail_count = 0
    filtered_count = 0
    
    for i, file_path in enumerate(all_files[:max_display]):
        filename = Path(file_path).name
        print(f"\n{i+1}. {filename}")
        
        try:
            match = date_regex.search(filename)
            if match:
                date_str = match.group(1)
                file_date = datetime.strptime(date_str, date_format)
                
                # Check date range if provided
                in_range = True
                if start_date and file_date < start_date:
                    in_range = False
                if end_date and file_date > end_date:
                    in_range = False
                
                if in_range:
                    print(f"   ✓ Date: {file_date} [IN RANGE]")
                    success_count += 1
                else:
                    print(f"   ○ Date: {file_date} [OUT OF RANGE]")
                    filtered_count += 1
            else:
                print(f"   ✗ No match")
                fail_count += 1
        except Exception as e:
            print(f"   ✗ Error: {e}")
            fail_count += 1
    
    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY:")
    print(f"  Successfully parsed: {success_count} files")
    if start_date or end_date:
        print(f"  Filtered (out of range): {filtered_count} files")
    print(f"  Failed to parse: {fail_count} files")
    if len(all_files) > max_display:
        print(f"  (Only tested first {max_display} of {len(all_files)} files)")
    print(f"{'='*70}")


# =============================================================================
# Test cases
# =============================================================================

def test_imerg_format():
    """Test original IMERG filename format."""
    print("\n" + "="*70)
    print("TEST CASE: IMERG Format")
    print("="*70)
    
    test_file_pattern(
        test_filename="3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4",
        date_pattern=r'\.(\d{8})-',
        date_format='%Y%m%d'
    )


def test_ir_imerg_format():
    """Test ir_imerg filename format."""
    print("\n" + "="*70)
    print("TEST CASE: ir_imerg Format")
    print("="*70)
    
    test_file_pattern(
        test_filename="merg_2020123108_10km-pixel.nc",
        date_pattern=r'_(\d{10})_',
        date_format='%Y%m%d%H'
    )


def test_generic_format():
    """Test generic filename with dashes."""
    print("\n" + "="*70)
    print("TEST CASE: Generic Format with Dashes")
    print("="*70)
    
    test_file_pattern(
        test_filename="precipitation_2020-01-01_v2.nc",
        date_pattern=r'_(\d{4}-\d{2}-\d{2})_',
        date_format='%Y-%m-%d'
    )


if __name__ == "__main__":
    print("\n" + "🔍 File Pattern Testing Tool" + "\n")
    
    # Run individual format tests
    test_imerg_format()
    test_ir_imerg_format()
    test_generic_format()
    
    # Example: Test scanning an actual directory
    # Uncomment and modify for your use case:
    
    test_directory_scan(
        base_dir="/pscratch/sd/w/wcmca1/GPM/IR_IMERG_Combined_V07B",
        date_pattern=r'_(\d{10})_',
        date_format='%Y%m%d%H',
        use_year_subdirs=True,
        file_glob='*.nc',
        start_date=datetime(2020, 1, 1, 0),
        end_date=datetime(2020, 1, 2, 23),
        max_display=20
    )
    
    print("\n✅ All tests completed!")
    print("\nTo test your own directory:")
    print("  1. Edit the test_directory_scan() call at the bottom of this script")
    print("  2. Uncomment it")
    print("  3. Run the script again")
