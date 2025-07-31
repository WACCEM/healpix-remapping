#!/bin/bash
#SBATCH --job-name=imerg_healpix
#SBATCH --account=m4534
#SBATCH --qos=shared
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=450GB
#SBATCH --time=12:00:00
#SBATCH --constraint=cpu
#SBATCH --output=imerg_healpix_%j.out
#SBATCH --error=imerg_healpix_%j.err

# Script to submit IMERG to HEALPix processing job on Perlmutter
# Usage: sbatch submit_imerg_job.sh <start_date> <end_date> [zoom_level]
# Example: sbatch submit_imerg_job.sh 2020-01-01 2020-12-31 9

echo "Starting IMERG to HEALPix processing job"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Start time: $(date)"

# Load modules
module purge
module load conda
module load python/3.11

# Activate conda environment (adjust as needed)
source activate /global/common/software/m1867/python/hackathon

# Change to working directory
cd /global/homes/f/feng045/program/hackathon

# Print system info
echo "Python version: $(python --version)"
echo "Available memory: $(free -h | grep Mem)"
echo "CPU cores: $(nproc)"

# Get command line arguments passed to sbatch
START_DATE=${1:-"2020-01-01"}
END_DATE=${2:-"2020-12-31"}
ZOOM=${3:-9}

echo "Processing period: $START_DATE to $END_DATE"
echo "HEALPix zoom level: $ZOOM"

# Set up environment variables for optimal performance
export OMP_NUM_THREADS=8
export OPENBLAS_NUM_THREADS=8
export MKL_NUM_THREADS=8
export NUMBA_NUM_THREADS=8

# Run the processing
python launch_imerg_processing.py "$START_DATE" "$END_DATE" "$ZOOM"

echo "Job completed at: $(date)"
echo "Peak memory usage:"
sacct -j $SLURM_JOB_ID --format=JobID,MaxRSS,MaxVMSize
