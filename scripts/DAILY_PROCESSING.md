# Daily Processing Strategy for HEALPix Coarsening

This directory contains scripts for processing large HEALPix datasets by breaking them into daily chunks to avoid memory issues and fit SLURM queue requirements.

## Overview

The daily processing strategy:
1. **Splits full year into individual days** - Each day is processed separately
2. **Uses SLURM job arrays** - Single submission manages all tasks efficiently
3. **Reduces memory usage** - Only one day loaded at a time (~10-20 GB vs ~200+ GB)
4. **Enables parallelization** - Process up to 2 days simultaneously (remote data access limitation)
5. **Improves fault tolerance** - One failed day doesn't affect others
6. **SLURM-friendly** - Shorter jobs fit better in queue priorities

## Files

- **`coarsen_catalog_ifs.py`** - Main coarsening script (supports date ranges)
- **`submit_daily_coarsen_jobs.py`** - Generates task list and submits job array
- **`slurm_coarsen_daily.sh`** - SLURM job array script (reads from task list)
- **`check_daily_progress.py`** - Monitor processing progress
- **`task_list.txt`** - Auto-generated task list (one line per day)

## How It Works

### Task List Format
Each line in `task_list.txt` represents one day:
```
2020-01-01 2020-01-01 8 3
2020-01-02 2020-01-02 8 3
...
```
Format: `START_DATE END_DATE TARGET_ZOOM TIME_SUBSAMPLE_FACTOR`

### SLURM Job Array
- **Single submission** creates 366 array tasks (for full year)
- Each task processes one line from task list
- Max concurrent tasks controlled by `--array=1-366%2` (2 at a time for remote data)
- Logs: `logs/coarsen_<JobID>_<TaskID>.out`

## Quick Start

### Step 1: Generate Task List and Submit

Generate task list and submit job array for full year 2020:

```bash
cd /global/homes/f/feng045/program/hackathon/healpix-remapping/scripts

# Create logs directory
mkdir -p logs

# Generate task list and submit job array
python submit_daily_coarsen_jobs.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01 \
    --target_zoom 8 \
    --time_subsample_factor 3 \
    --max_concurrent 2
```

This creates `task_list.txt` with 366 lines and submits a single job array.

### Step 2: Monitor Progress

```bash
# Check SLURM queue (shows running and pending tasks)
squeue -u $USER

# Check specific job array
squeue -j <JobID>

# Check which days are complete
python check_daily_progress.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01
```

## Configuration Options

### `submit_daily_coarsen_jobs.py` Options

```bash
# Required
--start_date YYYY-MM-DD    # Start date for processing
--end_date YYYY-MM-DD      # End date for processing

# Processing parameters
--target_zoom 8            # Target zoom level (default: 8)
--time_subsample_factor 3  # Temporal subsampling (default: 3 for 3-hourly)

# SLURM parameters
--account m1867            # SLURM account (default: m1867)
--time_limit 02:00:00      # Job time limit per task (default: 2 hours)
--constraint cpu           # Node constraint (default: cpu)
--qos regular              # Quality of service (default: regular)
--max_concurrent 2         # Max simultaneous tasks (default: 2 for remote data access)

# File parameters
--task_list task_list.txt  # Task list filename (default: task_list.txt)
--log_dir ./logs           # Log directory (default: ./logs)
--output_dir /path/to/out  # Output directory for coarsened data

# Execution control
--generate_only            # Generate task list but don't submit
--slurm_script script.sh   # SLURM script to use (default: slurm_coarsen_daily.sh)
```

### Job Array Parameters in `slurm_coarsen_daily.sh`

The `#SBATCH --array=1-366%2` line controls:
- **Range**: `1-366` means tasks 1 through 366 (one per day in 2020)
- **Throttle**: `%2` means max 2 tasks running simultaneously (required for remote DKRZ server)

**Important:** The remote DKRZ data server has connection limits. Testing showed:
- 10 concurrent: All failed (502 Bad Gateway errors)
- 3 concurrent: Only 30% succeeded (3/10 tasks)
- **2 concurrent: 100% success rate** (5/5 tasks completed)

To modify, edit the script or override when submitting:
```bash
sbatch --array=1-31%2 slurm_coarsen_daily.sh  # 31 days, max 2 concurrent (recommended)
```

## Examples

### Process Full Year 2020

```bash
# Submit all 366 days (max 2 concurrent for remote data stability)
python submit_daily_coarsen_jobs.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01 \
    --target_zoom 8 \
    --time_subsample_factor 3 \
    --max_concurrent 2
```

### Process Just January (Testing)

```bash
# Test with one month, use recommended 2 concurrent tasks
python submit_daily_coarsen_jobs.py \
    --start_date 2020-01-01 \
    --end_date 2020-01-31 \
    --target_zoom 8 \
    --time_subsample_factor 3 \
    --max_concurrent 2
```

### Generate Task List Only (No Submission)

```bash
# Review task list before submitting
python submit_daily_coarsen_jobs.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01 \
    --generate_only

# Review the generated task list
head -20 task_list.txt

# Submit manually when ready
sbatch slurm_coarsen_daily.sh
```

### Custom Time Limit for Longer Processing

```bash
# If tasks need more than 2 hours
python submit_daily_coarsen_jobs.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01 \
    --time_limit 04:00:00  # 4 hours per task
```

### Resubmit Failed Tasks

```bash
# Find failed dates
python check_daily_progress.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01 \
    --generate_resubmit

# This will show commands to resubmit specific date ranges
# Or manually create a new task list with only failed dates:
# 2020-01-15 2020-01-15 8 3
# 2020-02-03 2020-02-03 8 3

# Then submit with custom array range
sbatch --array=1-2 slurm_coarsen_daily.sh
```

## Monitoring Jobs

```bash
# Check job array status (shows running/pending/completed tasks)
squeue -u $USER

# Check specific job array by ID
squeue -j <JobID>

# Detailed view of array tasks
squeue -j <JobID> -t RUNNING    # Running tasks only
squeue -j <JobID> -t PENDING    # Pending tasks only

# Count tasks by state
squeue -j <JobID> | tail -n +2 | wc -l  # Total tasks in queue

# View specific task log (while running)
tail -f logs/coarsen_<JobID>_<TaskID>.out

# Check completed jobs
sacct -j <JobID> --format=JobID,State,ExitCode,Elapsed -X

# Count completed tasks
sacct -j <JobID> --format=State -X | grep COMPLETED | wc -l

# Find failed tasks
sacct -j <JobID> --format=JobID,State,ExitCode -X | grep FAILED
```

## Managing Jobs

```bash
# Cancel entire job array
scancel <JobID>

# Cancel specific array tasks
scancel <JobID>_[10-20]     # Cancel tasks 10-20
scancel <JobID>_50          # Cancel task 50

# Cancel all running tasks for user
scancel -u $USER

# Hold job array (prevent pending tasks from starting)
scontrol hold <JobID>

# Release held job array
scontrol release <JobID>
```

## Output Files

Each daily job creates a Zarr file:

```
/pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf/
├── ifs_tco3999_rcbmf_3H_zoom8_20200101_20200101.zarr/
├── ifs_tco3999_rcbmf_3H_zoom8_20200102_20200102.zarr/
├── ifs_tco3999_rcbmf_3H_zoom8_20200103_20200103.zarr/
└── ...
```

## Troubleshooting

### Check Processing Progress

```bash
# See which days completed successfully
python check_daily_progress.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01

# Show complete files too
python check_daily_progress.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01 \
    --show_complete

# Generate resubmit commands for failed dates
python check_daily_progress.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01 \
    --generate_resubmit
```

### Job Fails with Memory Error

- Single day should fit easily in one node (~10-20 GB)
- If failing, check if processing correct date range:
  ```bash
  grep "Date Range:" logs/coarsen_*_123.out
  ```
- Increase time limit if running out of time:
  ```bash
  python submit_daily_coarsen_jobs.py ... --time_limit 04:00:00
  ```

### Tasks Stuck in Queue

- Check queue limits: `squeue -u $USER`
- Max 2 concurrent recommended for remote data access stability
- Try different QOS: `--qos debug` (30 min, 2 nodes, fast turnaround)

### Output Files Missing

Check logs for specific task:
```bash
# Find task ID from date (e.g., day 15 of year = task 15)
TASK_ID=15
JOB_ID=<your_job_id>

# Check output log
cat logs/coarsen_${JOB_ID}_${TASK_ID}.out

# Check error log
cat logs/coarsen_${JOB_ID}_${TASK_ID}.err

# Check exit code
sacct -j ${JOB_ID}_${TASK_ID} --format=JobID,State,ExitCode
```

### Resubmit Failed Tasks

Method 1: Create new task list with only failed dates
```bash
# Manually create failed_tasks.txt:
# 2020-01-15 2020-01-15 8 3
# 2020-02-03 2020-02-03 8 3

# Submit with custom task list
sbatch --array=1-2 --export=TASK_LIST=failed_tasks.txt slurm_coarsen_daily.sh
```

Method 2: Use check_daily_progress.py
```bash
# Get resubmit commands
python check_daily_progress.py \
    --start_date 2020-01-01 \
    --end_date 2021-03-01 \
    --generate_resubmit

# Follow the generated commands
```

## Post-Processing: Combine Daily Files

After all daily jobs complete, you can combine them into monthly or yearly files:

```python
import xarray as xr
from pathlib import Path
import zarr

output_dir = Path("/pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf/")

# Find all daily files for January 2020
daily_files = sorted(output_dir.glob("*_zoom8_202001*.zarr"))

# Open all as a single dataset
ds_combined = xr.open_mfdataset(
    daily_files,
    engine='zarr',
    combine='by_coords',
    parallel=True
)

# Write to monthly file
monthly_file = output_dir / "ifs_tco3999_rcbmf_3H_zoom8_202001.zarr"
ds_combined.to_zarr(monthly_file, mode='w', consolidated=True)
```

## Troubleshooting

### Job Fails with Memory Error

- Reduce spatial domain or variables
- Increase time limit
- Request more nodes (though single day should fit on 1 node)

### Jobs Stuck in Queue

- Use `--qos debug` for quick testing (30 min limit, 2 nodes max)
- Use `--constraint cpu` for CPU nodes (more available)
- Check queue: `squeue -u $USER`

### Output Files Missing

Check logs:
```bash
# Find failed jobs
grep -l "FAILED" logs/coarsen_*.out

# Check specific date
cat logs/coarsen_20200101_*.err
```

### Resubmit Failed Jobs

```bash
# Find dates with no output
for date in {01..31}; do
    file="ifs_tco3999_rcbmf_3H_zoom8_202001${date}_202001${date}.zarr"
    if [ ! -d "$OUTPUT_DIR/$file" ]; then
        echo "Missing: 2020-01-${date}"
    fi
done

# Resubmit specific dates
sbatch --export=START_DATE=2020-01-15,END_DATE=2020-01-15 slurm_coarsen_daily.sh
```

## Performance Estimates

Based on test results with remote DKRZ data access:

- **Single day processing**: ~30-38 minutes (avg 35 min)
- **Memory per day**: ~5-7 GB (well within 20GB allocation)
- **Job array advantages**:
  - Single submission (1 job array vs 366 individual jobs)
  - Cleaner queue (`squeue` shows 1 array job, not 366 separate jobs)
  - Easier management (cancel entire array with one command)
  - SLURM scheduling optimization
- **Parallel execution**: Max 2 days simultaneously (remote data access constraint)
- **Total wall time for full year**: ~106 hours (~4.4 days) with 2 concurrent tasks

**Remote Data Access Constraint:**
The DKRZ cloud object store limits concurrent connections. Testing showed:
- 10 concurrent → 100% failure rate (502 Bad Gateway errors)
- 3 concurrent → 30% success rate (3/10 tasks completed)
- **2 concurrent → 100% success rate** (5/5 tasks completed)

## Best Practices

1. **Always test first**: Run one day interactively before submitting job array
   ```bash
   python coarsen_catalog_ifs.py --start_date 2020-01-01 --end_date 2020-01-01 \
       --target_zoom 8 --time_subsample_factor 3 --overwrite
   ```

2. **Use generate_only first**: Review task list before submitting
   ```bash
   python submit_daily_coarsen_jobs.py --start_date ... --end_date ... --generate_only
   cat task_list.txt | head -10
   ```

3. **Start small**: Test with one month before full year
   ```bash
   python submit_daily_coarsen_jobs.py --start_date 2020-01-01 --end_date 2020-01-31
   ```

4. **Monitor early**: Watch first few tasks to ensure they complete
   ```bash
   squeue -j <JobID> -t RUNNING
   tail -f logs/coarsen_<JobID>_1.out
   ```

5. **Check progress regularly**: Use check_daily_progress.py
   ```bash
   python check_daily_progress.py --start_date ... --end_date ...
   ```

6. **Keep task list**: Don't delete task_list.txt - needed for troubleshooting

7. **Use recommended concurrency**: `--max_concurrent 2` is optimal for remote DKRZ data
   - Higher values cause 502 Bad Gateway errors
   - Lower values unnecessarily slow processing

## Advantages of Job Arrays vs Individual Jobs

| Aspect | Job Arrays | Individual Jobs |
|--------|-----------|-----------------|
| Submission | 1 command | 366 commands |
| Queue visibility | 1 entry | 366 entries |
| Cancellation | `scancel <JobID>` | `scancel` 366 times |
| Logs | `logs/coarsen_<JobID>_*.out` | `logs/coarsen_<date>_*.out` |
| SLURM overhead | Minimal | Significant |
| Script management | 1 file (task_list.txt) | 366 files |
| Resource limits | Array-aware limits | Individual job limits |
| Scheduling priority | Better optimization | Random |

## Additional Resources

- [NERSC Perlmutter Documentation](https://docs.nersc.gov/systems/perlmutter/)
- [SLURM Documentation](https://slurm.schedmd.com/documentation.html)
- [Xarray Chunking Guide](https://docs.xarray.dev/en/stable/user-guide/dask.html)
