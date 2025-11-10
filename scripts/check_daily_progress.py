#!/usr/bin/env python3
"""
Check which daily files have been processed and identify missing dates.

Usage:
    python check_daily_progress.py --start_date 2020-01-01 --end_date 2020-12-31
"""

import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd


def generate_expected_files(start_date, end_date, base_name, zoom, time_res, output_dir):
    """
    Generate list of expected output files for date range.
    
    Returns:
    --------
    dict : Dictionary mapping date strings to expected file paths
    """
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    expected_files = {}
    current_dt = start_dt
    
    while current_dt <= end_dt:
        date_str = current_dt.strftime('%Y%m%d')
        filename = f"{base_name}_{time_res}_zoom{zoom}_{date_str}_{date_str}.zarr"
        expected_files[date_str] = output_dir / filename
        current_dt += timedelta(days=1)
    
    return expected_files


def check_file_completeness(zarr_path):
    """
    Check if a Zarr file is complete and valid.
    
    Returns:
    --------
    tuple : (is_complete, size_mb, error_msg)
    """
    if not zarr_path.exists():
        return False, 0, "File does not exist"
    
    # Check if .zmetadata exists (indicates consolidated metadata)
    zmetadata = zarr_path / ".zmetadata"
    if not zmetadata.exists():
        return False, 0, "Missing .zmetadata (incomplete write?)"
    
    # Calculate directory size
    try:
        size_bytes = sum(f.stat().st_size for f in zarr_path.rglob('*') if f.is_file())
        size_mb = size_bytes / (1024 * 1024)
    except Exception as e:
        return False, 0, f"Error calculating size: {e}"
    
    # Basic size check (adjust threshold as needed)
    if size_mb < 1:
        return False, size_mb, f"File too small ({size_mb:.1f} MB)"
    
    return True, size_mb, None


def main():
    parser = argparse.ArgumentParser(
        description='Check progress of daily HEALPix coarsening jobs.'
    )
    
    parser.add_argument('--start_date', required=True,
                        help='Start date (format: YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True,
                        help='End date (format: YYYY-MM-DD)')
    
    parser.add_argument('--output_dir', 
                        default='/pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf/',
                        help='Output directory for coarsened data')
    
    parser.add_argument('--base_name', default='ifs_tco3999_rcbmf',
                        help='Base name for output files (default: ifs_tco3999_rcbmf)')
    parser.add_argument('--zoom', type=int, default=8,
                        help='Zoom level (default: 8)')
    parser.add_argument('--time_res', default='3H',
                        help='Time resolution (default: 3H)')
    
    parser.add_argument('--show_complete', action='store_true',
                        help='Show complete files in addition to missing/incomplete')
    parser.add_argument('--generate_resubmit', action='store_true',
                        help='Generate commands to resubmit missing dates')
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    
    if not output_dir.exists():
        print(f"ERROR: Output directory does not exist: {output_dir}")
        return 1
    
    print(f"Checking daily processing progress...")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Output directory: {output_dir}")
    print(f"Expected pattern: {args.base_name}_{args.time_res}_zoom{args.zoom}_YYYYMMDD_YYYYMMDD.zarr")
    print()
    
    # Generate expected files
    expected_files = generate_expected_files(
        args.start_date, args.end_date, 
        args.base_name, args.zoom, args.time_res,
        output_dir
    )
    
    total_days = len(expected_files)
    complete_files = []
    incomplete_files = []
    missing_files = []
    
    # Check each expected file
    for date_str, file_path in expected_files.items():
        is_complete, size_mb, error_msg = check_file_completeness(file_path)
        
        if is_complete:
            complete_files.append((date_str, file_path, size_mb))
        elif file_path.exists():
            incomplete_files.append((date_str, file_path, error_msg))
        else:
            missing_files.append((date_str, file_path))
    
    # Print summary
    print(f"{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"Total days: {total_days}")
    print(f"Complete: {len(complete_files)} ({len(complete_files)/total_days*100:.1f}%)")
    print(f"Incomplete: {len(incomplete_files)} ({len(incomplete_files)/total_days*100:.1f}%)")
    print(f"Missing: {len(missing_files)} ({len(missing_files)/total_days*100:.1f}%)")
    print()
    
    # Show complete files if requested
    if args.show_complete and complete_files:
        print(f"{'='*70}")
        print(f"COMPLETE FILES ({len(complete_files)})")
        print(f"{'='*70}")
        total_size = sum(size for _, _, size in complete_files)
        print(f"Total size: {total_size/1024:.1f} GB")
        print(f"Average size: {total_size/len(complete_files):.1f} MB")
        print()
        
        if len(complete_files) <= 20:
            for date_str, file_path, size_mb in complete_files:
                print(f"✅ {date_str}: {file_path.name} ({size_mb:.1f} MB)")
        else:
            print("First 10 files:")
            for date_str, file_path, size_mb in complete_files[:10]:
                print(f"✅ {date_str}: {file_path.name} ({size_mb:.1f} MB)")
            print(f"... ({len(complete_files)-10} more)")
        print()
    
    # Show incomplete files
    if incomplete_files:
        print(f"{'='*70}")
        print(f"INCOMPLETE FILES ({len(incomplete_files)})")
        print(f"{'='*70}")
        for date_str, file_path, error_msg in incomplete_files:
            print(f"⚠️  {date_str}: {file_path.name}")
            print(f"    Error: {error_msg}")
        print()
    
    # Show missing files
    if missing_files:
        print(f"{'='*70}")
        print(f"MISSING FILES ({len(missing_files)})")
        print(f"{'='*70}")
        for date_str, file_path in missing_files:
            print(f"❌ {date_str}: {file_path.name}")
        print()
    
    # Generate resubmit commands if requested
    if args.generate_resubmit and (missing_files or incomplete_files):
        print(f"{'='*70}")
        print(f"RESUBMIT COMMANDS")
        print(f"{'='*70}")
        
        failed_dates = []
        for date_str, _ in missing_files:
            # Convert YYYYMMDD to YYYY-MM-DD
            date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            failed_dates.append(date)
        
        for date_str, _, _ in incomplete_files:
            date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            failed_dates.append(date)
        
        print("# Individual sbatch commands:")
        for date in sorted(failed_dates)[:10]:  # Show first 10
            print(f"sbatch --export=START_DATE={date},END_DATE={date} slurm_coarsen_daily.sh")
        
        if len(failed_dates) > 10:
            print(f"# ... ({len(failed_dates)-10} more dates)")
        
        print()
        print("# Or resubmit all at once with submit_daily_coarsen_jobs.py:")
        
        # Group consecutive dates into ranges
        failed_dates_sorted = sorted(failed_dates)
        if failed_dates_sorted:
            ranges = []
            start = failed_dates_sorted[0]
            prev = failed_dates_sorted[0]
            
            for date in failed_dates_sorted[1:]:
                curr_dt = pd.to_datetime(date)
                prev_dt = pd.to_datetime(prev)
                
                if (curr_dt - prev_dt).days == 1:
                    prev = date
                else:
                    ranges.append((start, prev))
                    start = date
                    prev = date
            
            ranges.append((start, prev))
            
            for start, end in ranges:
                print(f"python submit_daily_coarsen_jobs.py --start_date {start} --end_date {end}")
        
        print()
    
    # Return exit code
    if incomplete_files or missing_files:
        return 1
    else:
        print("✅ All files complete!")
        return 0


if __name__ == '__main__':
    exit(main())
