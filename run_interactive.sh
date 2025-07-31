#!/bin/bash
# Interactive wrapper for IMERG processing with environment activation
# Usage: ./run_interactive.sh <start_date> <end_date> [zoom_level]
# Example: ./run_interactive.sh 2020-01-01 2020-01-31 9

echo "IMERG Processing - Interactive Mode"
echo "==================================="

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <start_date> <end_date> [zoom_level]"
    echo "Example: $0 2020-01-01 2020-01-31 9"
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
echo "Starting IMERG processing..."
echo "Start date: $1"
echo "End date: $2"
echo "Zoom level: ${3:-9}"

python launch_imerg_processing.py "$1" "$2" "${3:-9}"

echo ""
echo "Processing completed!"
