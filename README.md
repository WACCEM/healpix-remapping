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
# Data paths
input_base_dir: "/pscratch/sd/w/wcmca1/GPM/IMERG_V07B_hpss/"
output_base_dir: "/pscratch/sd/w/wcmca1/GPM/IMERG_V07B_hpss/healpix_test/"
output_basename: "IMERG_V7"
weights_dir: "/pscratch/sd/w/wcmca1/GPM/weights/"

# Processing parameters
default_zoom: 9
time_chunk_size: 24  # 1 day worth of 1-hour data
time_average: "1h"   # Temporal averaging: 30min → 1h
convert_time: True   # Convert cftime to datetime64 for pandas compatibility

# Dask configuration for NERSC Perlmutter
dask:
  n_workers: 16              # 16 workers for NUMA topology
  threads_per_worker: 8      # 128 total threads
  memory_limit: "30GB"       # 480GB total memory
```

### 4. Process Data

**Single day (interactive):**
```bash
python launch_imerg_processing.py 2020-01-01 2020-01-01 9
```

**Single month:**
```bash
python launch_imerg_processing.py 2020-01-01 2020-01-31 9
```

**Full year (batch job):**
```bash
sbatch submit_imerg_job.sh 2020-01-01 2020-12-31 9
```

**With overwrite option:**
```bash
python launch_imerg_processing.py 2020-01-01 2020-01-31 9 --overwrite
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
- **Time chunks**: 24 time steps (1 day of 1-hour averaged data)
- **Spatial chunks**: Computed automatically per zoom level
  - Zoom 9: 262,144 cells/chunk (12 spatial chunks total)
- **Temporal averaging**: 30-minute IMERG → 1-hour averaged output

## Memory & Performance

**Zoom 9 Processing (IMERG native resolution with 1h averaging):**
- Memory: ~24MB per chunk (24 time steps × 262K cells × 4 bytes)
- Processing: ~4-5 seconds per day on Perlmutter
- Output: ~0.8GB per 3 days in compressed Zarr format
- Temporal averaging: 30-minute → 1-hour reduces data volume by 50%

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

### With Overwrite Option
```bash
# Overwrite existing files
python launch_imerg_processing.py 2020-01-01 2020-01-07 9 --overwrite
```

## Output Format

The processed data is saved as Zarr files with:
- **Naming**: `IMERG_V7_1H_zoom{zoom}_{start_date}_{end_date}.zarr`
- **Structure**: Time series of HEALPix-remapped precipitation data
- **Compression**: zstd level 3 for optimal size/speed balance
- **Temporal resolution**: 1-hour averaged from 30-minute IMERG
- **Chunks**: Optimized for subsequent analysis workflows

**Zarr Structure:**
```
output.zarr/
├── precipitation/        # Main data variable (1-hour averaged)
├── time/                # Time coordinates (datetime64)
├── cell/                # HEALPix cell indices
├── crs/                 # Grid mapping metadata
└── .zmetadata           # Consolidated metadata
```

**Metadata includes:**
- HEALPix parameters (nside, order, nest=True)
- Original grid information
- Processing timestamps
- Temporal averaging information
- Chunk configuration

## Performance Tips

1. **Memory**: Use time_chunk_size=24 for optimal balance with 1h averaging
2. **I/O**: Ensure fast storage for input/output paths
3. **Parallelism**: Leverage 16 workers × 8 threads on Perlmutter nodes
4. **Caching**: Reuse weight files across processing runs
5. **Temporal averaging**: Use "1h" averaging to reduce data volume by 50%

## Troubleshooting

**Common Issues:**
- Memory errors: Reduce time_chunk_size in config (try 12 or 6)
- Slow I/O: Check storage system performance
- Missing dependencies: Run `python test_setup.py`
- File path errors: Check for double slashes in output paths

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
