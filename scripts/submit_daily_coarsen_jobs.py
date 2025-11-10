#!/usr/bin/env python3
"""
Generate task list and submit job array for coarsening HEALPix data.

This script creates a task list file where each line represents one day of processing,
then submits a single SLURM job array to process all days in parallel.

Usage:
    python submit_daily_coarsen_jobs.py --start_date 2020-01-01 --end_date 2021-03-01 [options]
    
Examples:
    # Generate task list and submit job array for full year 2020
    python submit_daily_coarsen_jobs.py --start_date 2020-01-01 --end_date 2021-03-01
    
    # Only generate task list without submitting (for review)
    python submit_daily_coarsen_jobs.py --start_date 2020-01-01 --end_date 2021-03-01 --generate_only
    
    # Process with custom parameters and throttle to 2 simultaneous jobs (recommended for remote data)
    python submit_daily_coarsen_jobs.py --start_date 2020-01-01 --end_date 2021-03-01 \
        --target_zoom 8 --time_subsample_factor 3 --max_concurrent 2
"""

import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd


def generate_date_range(start_date, end_date):
    """
    Generate list of daily date ranges.
    
    Parameters:
    -----------
    start_date : str
        Start date (format: YYYY-MM-DD)
    end_date : str
        End date (format: YYYY-MM-DD)
        
    Returns:
    --------
    list : List of (start_date, end_date) tuples for each day
    """
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    date_ranges = []
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime('%Y-%m-%d')
        date_ranges.append((date_str, date_str))
        current_dt += timedelta(days=1)
    
    return date_ranges


def generate_date_range(start_date, end_date):
    """
    Generate list of daily date ranges.
    
    Parameters:
    -----------
    start_date : str
        Start date (format: YYYY-MM-DD)
    end_date : str
        End date (format: YYYY-MM-DD)
        
    Returns:
    --------
    list : List of (start_date, end_date) tuples for each day
    """
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    date_ranges = []
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime('%Y-%m-%d')
        date_ranges.append((date_str, date_str))
        current_dt += timedelta(days=1)
    
    return date_ranges


def create_task_list(date_ranges, target_zoom, time_subsample_factor, task_list_file):
    """
    Create a task list file for SLURM job array.
    
    Each line contains: START_DATE END_DATE TARGET_ZOOM TIME_SUBSAMPLE_FACTOR
    
    Parameters:
    -----------
    date_ranges : list
        List of (start_date, end_date) tuples
    target_zoom : int
        Target zoom level
    time_subsample_factor : int
        Temporal subsampling factor
    task_list_file : Path
        Output file path for task list
        
    Returns:
    --------
    int : Number of tasks written
    """
    with open(task_list_file, 'w') as f:
        for start_date, end_date in date_ranges:
            f.write(f"{start_date} {end_date} {target_zoom} {time_subsample_factor}\n")
    
    return len(date_ranges)


def main():
    parser = argparse.ArgumentParser(
        description='Generate task list and submit job array for HEALPix coarsening.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Required arguments
    parser.add_argument('--start_date', required=True,
                        help='Start date (format: YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True,
                        help='End date (format: YYYY-MM-DD)')
    
    # Optional arguments
    parser.add_argument('--target_zoom', type=int, default=8,
                        help='Target zoom level (default: 8)')
    parser.add_argument('--time_subsample_factor', type=int, default=3,
                        help='Temporal subsampling factor (default: 3 for 3-hourly)')
    
    # SLURM parameters
    parser.add_argument('--account', default='m1867',
                        help='SLURM account (default: m1867)')
    parser.add_argument('--time_limit', default='01:00:00',
                        help='Job time limit (default: 01:00:00 for shared queue)')
    parser.add_argument('--constraint', default='cpu',
                        help='Node constraint (default: cpu)')
    parser.add_argument('--qos', default='shared',
                        help='Quality of service (default: shared)')
    parser.add_argument('--mem', default='20GB',
                        help='Memory per task (default: 20GB)')
    parser.add_argument('--cpus_per_task', type=int, default=1,
                        help='CPUs per task (default: 1)')
    parser.add_argument('--max_concurrent', type=int, default=2,
                        help='Maximum number of concurrent jobs (default: 2 for stable remote data access)')
    
    # Output files
    parser.add_argument('--task_list', default='task_list.txt',
                        help='Task list file name (default: task_list.txt)')
    parser.add_argument('--log_dir', default='./logs',
                        help='Directory for job logs (default: ./logs)')
    parser.add_argument('--output_dir', 
                        default='/pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf/',
                        help='Output directory for coarsened data')
    
    # Execution control
    parser.add_argument('--generate_only', action='store_true',
                        help='Generate task list but do not submit job array')
    parser.add_argument('--slurm_script', default='slurm_coarsen_daily.sh',
                        help='SLURM script to submit (default: slurm_coarsen_daily.sh)')
    
    args = parser.parse_args()
    
    # Create log directory
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Get path to SLURM script
    script_dir = Path(__file__).parent
    slurm_script = script_dir / args.slurm_script
    if not slurm_script.exists():
        print(f"ERROR: SLURM script not found: {slurm_script}")
        return 1
    
    # Generate date ranges
    date_ranges = generate_date_range(args.start_date, args.end_date)
    total_jobs = len(date_ranges)
    
    print(f"Generating task list for {total_jobs} days: {args.start_date} to {args.end_date}")
    print(f"Target zoom: {args.target_zoom}")
    print(f"Time subsample factor: {args.time_subsample_factor}")
    print(f"Output directory: {args.output_dir}")
    print(f"SLURM account: {args.account}")
    print(f"Time limit: {args.time_limit}")
    print(f"Max concurrent jobs: {args.max_concurrent}")
    print()
    
    # Create task list file
    task_list_file = script_dir / args.task_list
    num_tasks = create_task_list(date_ranges, args.target_zoom, 
                                  args.time_subsample_factor, task_list_file)
    
    print(f"✅ Created task list: {task_list_file}")
    print(f"   Total tasks: {num_tasks}")
    
    # Show first few tasks
    print(f"\n   First 5 tasks:")
    with open(task_list_file, 'r') as f:
        for i, line in enumerate(f, 1):
            if i > 5:
                break
            print(f"   {i}: {line.strip()}")
    
    if num_tasks > 5:
        print(f"   ... ({num_tasks - 5} more tasks)")
    print()
    
    if args.generate_only:
        print("🔍 GENERATE ONLY mode - Task list created but job not submitted")
        print(f"\nTo submit manually, run:")
        print(f"  sbatch --array=1-{num_tasks}%{args.max_concurrent} \\")
        print(f"         --account={args.account} \\")
        print(f"         --time={args.time_limit} \\")
        print(f"         --constraint={args.constraint} \\")
        print(f"         --qos={args.qos} \\")
        print(f"         --export=TASK_LIST={task_list_file.name} \\")
        print(f"         {slurm_script}")
        return 0
    
    # Submit job array
    print("Submitting job array...")
    
    sbatch_cmd = [
        'sbatch',
        f'--array=1-{num_tasks}%{args.max_concurrent}',
        f'--account={args.account}',
        f'--time={args.time_limit}',
        f'--constraint={args.constraint}',
        f'--qos={args.qos}',
        f'--mem={args.mem}',
        f'--cpus-per-task={args.cpus_per_task}',
        f'--export=TASK_LIST={task_list_file.name},OUTPUT_DIR={args.output_dir}',
        str(slurm_script)
    ]
    
    try:
        result = subprocess.run(
            sbatch_cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=script_dir
        )
        
        # Parse job ID from output (e.g., "Submitted batch job 12345")
        output = result.stdout.strip()
        job_id = output.split()[-1]
        
        print(f"✅ Job array submitted successfully!")
        print(f"\n{'='*60}")
        print("JOB ARRAY DETAILS")
        print(f"{'='*60}")
        print(f"Job ID: {job_id}")
        print(f"Array size: {num_tasks} tasks")
        print(f"Max concurrent: {args.max_concurrent} tasks")
        print(f"Task list: {task_list_file}")
        print(f"Log directory: {log_dir}/")
        print(f"Log pattern: coarsen_{job_id}_<task_id>.out")
        print()
        print("MONITORING COMMANDS:")
        print(f"  Check status:    squeue -j {job_id}")
        print(f"  Check all tasks: squeue -u $USER -r")
        print(f"  Cancel job:      scancel {job_id}")
        print(f"  View logs:       ls -lh {log_dir}/coarsen_{job_id}_*.out")
        print()
        print("PROGRESS CHECKING:")
        print(f"  python check_daily_progress.py --start_date {args.start_date} --end_date {args.end_date}")
        print(f"{'='*60}")
        
        return 0
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to submit job array")
        print(f"Error: {e.stderr}")
        return 1


if __name__ == '__main__':
    exit(main())
