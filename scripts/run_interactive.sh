#!/bin/bash
# Interactive wrapper for SCREAM processing with environment activation
# Usage: ./run_interactive.sh <start_date> <end_date> [options]
# Example: ./run_interactive.sh 2019-09-01 2019-09-30 -c ../config/scream_ne1024_1H_config.yaml -z 9

echo "SCREAM Processing - Interactive Mode"
echo "====================================="

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <start_date> <end_date> [options]"
    echo "Example: $0 2019-09-01 2019-09-30 -c ../config/scream_ne1024_1H_config.yaml -z 9"
    echo ""
    echo "Required arguments:"
    echo "  start_date    Start date (YYYY-MM-DD or YYYY-MM-DDTHH)"
    echo "  end_date      End date (YYYY-MM-DD or YYYY-MM-DDTHH)"
    echo ""
    echo "Optional arguments:"
    echo "  -c CONFIG     Path to config file (default: ../config/scream_ne1024_1H_config.yaml)"
    echo "  -z ZOOM       HEALPix zoom level (default: 9)"
    echo "  --overwrite   Overwrite existing output files"
    exit 1
fi

# Activate Python environment
echo "Activating Python environment..."
source activate /global/common/software/m1867/python/hackathon

# Check if activation was successful
if [ $? -ne 0 ]; then
    echo "Error: Failed to activate Python environment"
    echo "Please check the environment path: /global/common/software/m1867/python/hackathon"
    exit 1
fi

echo "✓ Python environment activated"
echo "Python version: $(python --version)"

# Run the processing
echo ""
echo "Starting SCREAM processing..."
echo "Arguments: $@"

python launch_scream_processing.py "$@"

echo ""
echo "Processing completed!"
