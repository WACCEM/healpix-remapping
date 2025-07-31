#!/bin/bash
# IMERG Processing Suite Initialization Script
# Run this to set up your environment for processing

echo "IMERG to HEALPix Processing Suite"
echo "================================="

# Check if we're in the right directory
if [[ ! -f "remap_imerg_to_zarr.py" ]]; then
    echo "Error: Please run this script from the remap_imerg directory"
    exit 1
fi

# Make scripts executable
chmod +x submit_imerg_job.sh
chmod +x launch_imerg_processing.py
chmod +x test_setup.py
chmod +x run_interactive.sh

echo "✓ Made scripts executable"

# Activate Python environment
echo ""
echo "Activating Python environment..."
source activate /global/common/software/m1867/python/hackathon

# Check Python environment
echo ""
echo "Checking Python environment..."
python -c "import sys; print(f'Python version: {sys.version}')"

# Run basic import test
echo ""
echo "Testing critical imports..."
python -c "
try:
    import xarray, dask, zarr, healpy
    print('✓ Core packages available')
except ImportError as e:
    print(f'✗ Missing package: {e}')
    exit(1)
"

# Check for easygems
echo ""
echo "Testing easygems..."
python -c "
try:
    import easygems.remap, easygems.healpix
    print('✓ EasyGEMS available')
except ImportError as e:
    print(f'✗ EasyGEMS not found: {e}')
    print('  Please ensure easygems is installed in your environment')
"

echo ""
echo "Setup complete! Next steps:"
echo "1. Edit imerg_config.yaml with your data paths"
echo "2. Run: python test_setup.py"
echo "3. Start processing with: python launch_imerg_processing.py"
echo ""
echo "For detailed usage, see README.md"
