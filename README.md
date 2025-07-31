# IMERG to HEALPix Processing Suite

This directory contains a complete suite of tools for processing IMERG precipitation data and remapping it to HEALPix grids, optimized for NERSC Perlmutter.

## Files Overview

### Core Processing Scripts
- **`remap_imerg_to_zarr.py`** - Main processing script with Dask optimization for large-scale multi-year IMERG processing
- **`chunking_calculator.py`** - Memory optimization and performance estimation utility

### Configuration and Launchers
- **`imerg_config.yaml`** - Centralized configuration file for data paths and processing parameters
- **`launch_imerg_processing.py`** - Simple command-line launcher script
- **`run_interactive.sh`** - Interactive wrapper with environment activation
- **`submit_imerg_job.sh`** - Slurm job script optimized for Perlmutter hardware

### Testing and Validation
- **`test_setup.py`** - Comprehensive test script to validate your environment setup

## Quick Start

### 1. Test Your Environment
```bash
cd /global/homes/f/feng045/program/hackathon/remap_imerg
python test_setup.py
```

### 2. Configure Data Paths
Edit `imerg_config.yaml` to set your actual data paths:
```yaml
input_base_dir: "/path/to/your/imerg/data/"
output_base_dir: "/path/to/output/zarr/files/"
weights_dir: "/path/to/weights/cache/"
```

### 3. Run Processing

#### Small datasets (interactive with auto environment):
```bash
./run_interactive.sh 2020-01-01 2020-01-31 9
```

#### Manual activation (if needed):
```bash
source activate /global/common/software/m1867/python/hackathon
python launch_imerg_processing.py 2020-01-01 2020-01-31 9
```

#### Large datasets (submit to Slurm):
```bash
sbatch submit_imerg_job.sh 2020-01-01 2022-12-31 9
```

## Features

### Hardware Optimization
- **Perlmutter-specific**: 8 workers × 8 threads = 64 cores
- **Memory management**: 60GB per worker (450GB total)
- **Thread control**: Prevents CPU oversubscription

### Processing Capabilities
- **Multi-file aggregation**: Combines multiple IMERG files into single dataset
- **Dask lazy evaluation**: Memory-efficient processing of large datasets
- **HEALPix remapping**: Using easygems.remap with Delaunay triangulation
- **Zarr output**: Compressed, chunked storage format
- **Smart filtering**: Automatically skips coordinate bounds variables

### Performance Features
- **Chunked processing**: Configurable time and spatial chunks
- **Parallel execution**: Dask distributed computing
- **Memory monitoring**: Built-in usage tracking and optimization
- **Progress tracking**: Real-time processing status

## Configuration Parameters

### Key Settings in `imerg_config.yaml`:
- **`default_zoom`**: HEALPix zoom level (9 recommended for ~4km resolution)
- **`time_chunk_size`**: Number of time steps per chunk (48 = 2 days)
- **`spatial_chunk_size`**: HEALPix cells per chunk (~1M recommended)
- **Dask settings**: Worker count, memory limits, thread configuration
- **Compression**: Zarr compression settings (zstd level 3)

## Example Usage

### Process 2020 data at zoom level 9:
```bash
# Test with one month first (easiest method)
./run_interactive.sh 2020-01-01 2020-01-31 9

# Manual method
source activate /global/common/software/m1867/python/hackathon
python launch_imerg_processing.py 2020-01-01 2020-01-31 9

# Full year via Slurm
sbatch submit_imerg_job.sh 2020-01-01 2020-12-31 9
```

### Memory estimation:
```bash
python chunking_calculator.py --zoom 9 --time-steps 17520 --variables 3
```

## Output Format

The processed data is saved as Zarr files with:
- **Naming**: `imerg_healpix_zoom{zoom}_{start_date}_{end_date}.zarr`
- **Structure**: Time series of HEALPix-remapped precipitation data
- **Compression**: zstd level 3 for optimal size/speed balance
- **Chunks**: Optimized for subsequent analysis workflows

## Dependencies

Required Python packages:
- `xarray`, `dask`, `zarr`
- `healpy`, `easygems`
- `numpy`, `scipy`
- `pyyaml` (for configuration)

## Troubleshooting

1. **Import errors**: Run `test_setup.py` to check all dependencies
2. **Memory issues**: Adjust chunk sizes in configuration
3. **Performance**: Use `chunking_calculator.py` for optimization
4. **Slurm issues**: Check job logs in `imerg_healpix_*.out` files

## Performance Notes

- **Optimal zoom levels**: 8-10 for most applications (1-4km resolution)
- **Memory scaling**: ~8GB per million HEALPix cells
- **Processing time**: ~1-2 hours per year of IMERG data at zoom 9
- **Storage**: ~50% compression ratio with zstd

## Support

For issues or questions, check:
1. Test results from `test_setup.py`
2. Slurm job logs for processing errors
3. Memory usage reports from `chunking_calculator.py`
