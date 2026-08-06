#!/bin/bash
#SBATCH --job-name=gsmap_healpix
#SBATCH --account=m1867
#SBATCH --qos=regular
#SBATCH --constraint=cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --time=06:00:00
#SBATCH --array=0-14%5
#SBATCH --output=logs/gsmap_%A_%a.out
#SBATCH --error=logs/gsmap_%A_%a.err

# SLURM job array script for GsMAP -> HEALPix processing: one array task per
# year, all writing in parallel into ONE pre-existing Zarr store (via
# --region-store), instead of each task writing (and later merging) its own
# separate store.
#
# Prerequisites:
#   1. logs/ directory must already exist (sbatch writes --output there
#      before the script body runs)
#   2. The target store must already be initialized:
#        python init_healpix_store.py -c ../config/gsmap_config.yaml \
#            --start-year 2010 --end-year 2024 -z 9 -o "$STORE"
#
# Usage:
#   sbatch --export=STORE=/path/to/store.zarr submit_gsmap_array.sh
#
#   # Override the year range (default: 2010 + SLURM_ARRAY_TASK_ID, i.e. the
#   # array index IS the offset from START_YEAR - keep --array and
#   # START_YEAR consistent):
#   sbatch --export=STORE=/path/to/store.zarr,START_YEAR=2015 \
#       --array=0-9%5 submit_gsmap_array.sh
#
#   # Resubmit only specific failed years (see check_healpix_store.py output),
#   # e.g. array indices 3 and 7 (= years 2013 and 2017 for START_YEAR=2010):
#   sbatch --export=STORE=/path/to/store.zarr --array=3,7%2 submit_gsmap_array.sh
#
# The --array %N suffix throttles concurrency (N tasks running at once).
# Each year is fully independent and idempotent - a failed/resubmitted task
# only ever touches its own year's chunk-aligned time region.

# ============================================================================
# CONFIGURATION - override via --export= at submit time as shown above
# ============================================================================

STORE="${STORE:?Error: must pass --export=STORE=/path/to/store.zarr}"
START_YEAR="${START_YEAR:-2010}"
ZOOM="${ZOOM:-9}"
CONFIG="${CONFIG:-../config/gsmap_config.yaml}"

SCRIPT_DIR="/global/homes/f/feng045/program/hackathon/healpix-remapping/scripts"
CONDA_ENV="/global/common/software/m1867/python/hackathon"

# ============================================================================
# JOB EXECUTION - generally no need to modify below this line
# ============================================================================

YEAR=$((START_YEAR + SLURM_ARRAY_TASK_ID))

echo "========================================"
echo "SLURM Job Array Information"
echo "========================================"
echo "Job ID: $SLURM_JOB_ID"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID  ->  Year: $YEAR"
echo "Node: $(hostname)"
echo "Start Time: $(date)"
echo ""
echo "Region store: $STORE"
echo "Zoom: $ZOOM"
echo "Config: $CONFIG"
echo "========================================"
echo ""

# Load modules and activate environment
module purge
module load conda
module load python/3.11
source activate "$CONDA_ENV"
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to activate conda environment: $CONDA_ENV"
    exit 1
fi
echo "Python: $(which python)"
echo "Python version: $(python --version)"
echo "Available memory: $(free -h | grep Mem)"
echo "CPU cores: $(nproc)"
echo ""

cd "$SCRIPT_DIR" || exit 1

# 16 dask workers x 8 threads = 128 cores/node (matches config's dask.n_workers)
export OMP_NUM_THREADS=8
export OPENBLAS_NUM_THREADS=8
export MKL_NUM_THREADS=8
export NUMBA_NUM_THREADS=8

echo "========================================"
echo "Processing year $YEAR into region store..."
echo "========================================"
echo ""

python launch_gsmap_processing.py "${YEAR}-01-01" "${YEAR}-12-31" \
    -z "$ZOOM" \
    -c "$CONFIG" \
    --region-store "$STORE"

EXIT_CODE=$?

echo ""
echo "========================================"
echo "Job Completion - Year $YEAR"
echo "========================================"
echo "End Time: $(date)"
echo "Exit Code: $EXIT_CODE"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Status: SUCCESS ✅"
else
    echo "Status: FAILED ❌"
fi
echo "Peak memory usage:"
sacct -j "$SLURM_JOB_ID" --format=JobID,MaxRSS,MaxVMSize
echo "========================================"

exit $EXIT_CODE
