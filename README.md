# IMERG to HEALPix Remapping Pipeline

[![GitHub](https://img.shields.io/github/license/WACCEM/remap_imerg_healpix)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

A high-performance, scalable pipeline for remapping IMERG precipitation data from regular lat/lon grids to HEALPix format. Optimized for NERSC Perlmutter with Dask parallel processing and Zarr cloud-native outputs.

## Features

- **Efficient Parallel Processing**: Dask-based processing with optimized memory usage
- **HEALPix Support**: Zoom levels 7-9 with automatic spatial chunking
- **Zarr Output**: Cloud-native format with optimal chunk sizes
- **NERSC Optimized**: Tuned for Perlmutter architecture with SLURM integration
- **Configuration Driven**: YAML-based configuration for easy parameter management
- **Comprehensive Testing**: Built-in validation and test suite

## Quick Start

### 1. Environment Setup
```bash
# On NERSC Perlmutter
source activate /global/common/software/m1867/python/hackathon
cd /path/to/remap_imerg_healpix
```

### 2. Test Installation
```bash
python test_setup.py
```

### 3. Configure Paths
Edit `imerg_config.yaml`:
```yaml
input_base_dir: "/path/to/your/imerg/data/"
output_base_dir: "/path/to/output/zarr/files/"
weights_dir: "/path/to/weights/cache/"
default_zoom: 9
time_chunk_size: 48  # 1 day of 30-min data
```

### 4. Process Data

**Single day (interactive):**
```bash
python launch_imerg_processing.py 2020-01-01 2020-01-01 9
```

**Multiple days (batch job):**
```bash
sbatch submit_imerg_job.sh 2020-01-01 2020-12-31 9
```

## Core Components

### Processing Pipeline
- **`remap_imerg_to_zarr.py`** - Main processing script with Dask optimization
- **`remap_tools.py`** - Core remapping functions with time-series handling
- **`zarr_tools.py`** - Zarr writing utilities for chunked output
- **`chunk_tools.py`** - Optimal chunking calculations based on zoom level

### Configuration & Execution
- **`imerg_config.yaml`** - Central configuration file
- **`launch_imerg_processing.py`** - Command-line launcher
- **`submit_imerg_job.sh`** - SLURM batch job script

### Testing & Validation
- **`test_setup.py`** - Environment and dependency validation
- **`test_*.py`** - Coordinate, data, and weights validation tests

## Processing Parameters

### HEALPix Zoom Levels
- **Zoom 7**: 196,608 cells (~0.4° resolution)
- **Zoom 8**: 786,432 cells (~0.2° resolution) 
- **Zoom 9**: 3,145,728 cells (~0.1° resolution) - **Recommended for IMERG**

### Chunking Strategy
- **Time chunks**: 48 time steps (1 day of 30-min data)
- **Spatial chunks**: Computed automatically per zoom level
  - Zoom 9: 262,144 cells/chunk (12 spatial chunks total)

## Memory & Performance

**Zoom 9 Processing (IMERG native resolution):**
- Memory: ~48MB per chunk (48 time steps × 262K cells × 4 bytes)
- Processing: ~9 seconds per day on Perlmutter
- Output: ~1.7GB per 3 days in compressed Zarr format

## File Structure

```
remap_imerg_healpix/
├── README.md                          # This file
├── imerg_config.yaml                  # Configuration
├── launch_imerg_processing.py         # Main launcher
├── remap_imerg_to_zarr.py            # Core processing
├── remap_tools.py                    # Remapping functions
├── zarr_tools.py                     # Zarr utilities
├── chunk_tools.py                    # Chunking calculations
├── submit_imerg_job.sh               # SLURM job script
├── test_setup.py                     # Environment tests
└── notebooks/                        # Development notebooks
```

## Examples

### Process a Single Month
```bash
python launch_imerg_processing.py 2020-01-01 2020-01-31 9
```

### Process Multiple Years (SLURM)
```bash
# Edit submit_imerg_job.sh with your date range
sbatch submit_imerg_job.sh 2019-01-01 2021-12-31 9
```

### Custom Configuration
```bash
# Use different time chunk size
python launch_imerg_processing.py 2020-01-01 2020-01-07 9 \
  --config custom_config.yaml
```

## Output Format

**Zarr Structure:**
```
output.zarr/
├── precipitation/        # Main data variable
├── time/                # Time coordinates
├── cell/                # HEALPix cell indices
├── crs/                 # Grid mapping metadata
└── .zmetadata           # Consolidated metadata
```

**Metadata includes:**
- HEALPix parameters (nside, order, nest=True)
- Original grid information
- Processing timestamps
- Chunk configuration

## Performance Tips

1. **Memory**: Use time_chunk_size=48 for optimal balance
2. **I/O**: Ensure fast storage for input/output paths
3. **Parallelism**: Leverage multiple workers on multi-core nodes
4. **Caching**: Reuse weight files across processing runs

## Troubleshooting

**Common Issues:**
- Memory errors: Reduce time_chunk_size in config
- Slow I/O: Check storage system performance
- Missing dependencies: Run `python test_setup.py`

**Getting Help:**
- Check logs in SLURM output files
- Validate environment with test scripts
- Review configuration in `imerg_config.yaml`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
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
