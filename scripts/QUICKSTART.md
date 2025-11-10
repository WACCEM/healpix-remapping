# Quick Start: Process Full Year with Job Arrays

## TL;DR

```bash
cd /global/homes/f/feng045/program/hackathon/healpix-remapping/scripts

# 1. Create logs directory
mkdir -p logs

# 2. Generate task list and submit job array (uses shared queue, max 2 concurrent)
python submit_daily_coarsen_jobs.py \
    --start_date 2020-01-01 \
    --end_date 2020-12-31 \
    --target_zoom 8 \
    --time_subsample_factor 3 \
    --max_concurrent 2

# 3. Monitor progress
squeue -u $USER
python check_daily_progress.py --start_date 2020-01-01 --end_date 2020-12-31
```

## What This Does

1. **Generates `task_list.txt`** - 366 lines, one per day:
   ```
   2020-01-01 2020-01-01 8 3
   2020-01-02 2020-01-02 8 3
   ...
   ```

2. **Submits SLURM job array on shared queue** - Single job with 366 tasks
   - Uses shared queue (1 core, 20 GB per task)
   - Each task processes one day (~35 minutes)
   - Max 2 tasks run simultaneously (remote data access limitation)
   - Logs: `logs/coarsen_<JobID>_<TaskID>.out`

3. **Processes in parallel** - All 366 days complete in ~106 hours (~4.4 days)

## Why Shared Queue?

Based on test results:
- ✅ Each task uses **1 core** (single-threaded)
- ✅ Each task uses **~5-7 GB memory**
- ✅ Takes **~30-38 minutes** per day
- ✅ **Remote data access requires throttling to 2 concurrent tasks**
- ✅ **More stable than 3+ concurrent** (prevents 502 Bad Gateway errors)

**Important:** The DKRZ remote server cannot handle more than 2 simultaneous connections reliably. Testing showed:
- 10 concurrent: All failed (502 errors)
- 3 concurrent: Only 3/10 succeeded
- 2 concurrent: 100% success rate (5/5 tasks completed)

See [REMOTE_DATA_NOTES.md](REMOTE_DATA_NOTES.md) for detailed analysis.

## Output

```
/pscratch/sd/w/wcmca1/hackathon/healpix/ifs_tco3999_rcbmf/
├── ifs_tco3999_rcbmf_3H_zoom8_20200101_20200101.zarr/
├── ifs_tco3999_rcbmf_3H_zoom8_20200102_20200102.zarr/
├── ...
└── ifs_tco3999_rcbmf_3H_zoom8_20201231_20201231.zarr/
```

## Monitoring

```bash
# Check job status
squeue -j <JobID>

# Check progress
python check_daily_progress.py --start_date 2020-01-01 --end_date 2020-12-31

# View logs
tail -f logs/coarsen_*_1.out   # First task
ls -lh logs/                    # All logs
```

## Cancel if Needed

```bash
# Cancel entire job array
scancel <JobID>
```

## Customize

```bash
# Different date range
python submit_daily_coarsen_jobs.py \
    --start_date 2020-06-01 \
    --end_date 2020-06-30 \
    --max_concurrent 2

# Change concurrent tasks (not recommended to go above 2 for remote data)
python submit_daily_coarsen_jobs.py \
    --start_date 2020-01-01 \
    --end_date 2020-12-31 \
    --max_concurrent 2

# Just generate task list (no submission)
python submit_daily_coarsen_jobs.py \
    --start_date 2020-01-01 \
    --end_date 2020-12-31 \
    --generate_only
```

## For More Details

See [DAILY_PROCESSING.md](DAILY_PROCESSING.md) for complete documentation.
