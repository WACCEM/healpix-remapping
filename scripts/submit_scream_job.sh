#!/bin/bash
#SBATCH --job-name=scream_healpix
#SBATCH --account=m4534
#SBATCH --qos=shared
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=450GB
#SBATCH --time=12:00:00
#SBATCH --constraint=cpu
#SBATCH --output=scream_healpix_%j.out
#SBATCH --error=scream_healpix_%j.err

# Script to submit SCREAM to HEALPix processing job on Perlmutter
# Usage: sbatch submit_scream_job.sh <start_date> <end_date> [options]
# Example: sbatch submit_scream_job.sh 2019-08-01 2020-09-01 -c ../config/scream_ne1024_1H_config.yaml -z 9

echo "Starting SCREAM to HEALPix processing job"
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
cd /global/homes/f/feng045/program/hackathon/healpix-remapping/scripts

# Print system info
echo "Python version: $(python --version)"
echo "Available memory: $(free -h | grep Mem)"
echo "CPU cores: $(nproc)"

# Check arguments
if [ $# -lt 2 ]; then
    echo "Error: Missing required arguments"
    echo "Usage: sbatch submit_scream_job.sh <start_date> <end_date> [options]"
    echo "Example: sbatch submit_scream_job.sh 2019-08-01 2020-09-01 -c ../config/scream_ne1024_1H_config.yaml -z 9"
    exit 1
fi

echo "Processing arguments: $@"

# Set up environment variables for optimal performance
export OMP_NUM_THREADS=8
export OPENBLAS_NUM_THREADS=8
export MKL_NUM_THREADS=8
export NUMBA_NUM_THREADS=8

# Run the processing
python launch_scream_processing.py "$@"

echo "Job completed at: $(date)"
echo "Peak memory usage:"
sacct -j $SLURM_JOB_ID --format=JobID,MaxRSS,MaxVMSize
