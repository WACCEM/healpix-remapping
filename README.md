# HEALPix Remapping Pipeline

[![GitHub](https://img.shields.io/github/license/WACCEM/healpix-remapping)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

A high-performance, scalable pipeline for remapping **any gridded lat/lon NetCDF dataset** to HEALPix format. Originally designed for IMERG precipitation data, now generalized to work with diverse datasets. Optimized for NERSC Perlmutter with Dask parallel processing and Zarr cloud-native outputs.

## ✨ What's New (Major Refactoring)

**Generalized Workflow** - No longer limited to IMERG! Now supports any gridded lat/lon dataset with flexible file pattern matching.

**Reorganized Project Structure** - Clean separation of library code, execution scripts, configuration files, and tests.

**Flexible Date/Time Specification** - Multiple date formats supported (YYYY-MM-DD, YYYY-MM-DDTHH, YYYY-MM-DD HH) with automatic end-of-day/hour extension.

**Test Utilities** - Easy-to-use test script (`test_file_pattern.py`) to validate your file pattern configuration before processing.

**Complete Examples** - See `scripts/example_usage.py` for 4 different dataset format examples.

## Features

- **Generalized for Any Dataset**: Flexible file pattern matching for diverse filename conventions
- **Efficient Parallel Processing**: Dask-based processing with optimized memory usage
- **HEALPix Support**: Any zoom level with automatic spatial chunking
- **Zarr Output**: Cloud-native format with customizable chunk sizes
- **NERSC Optimized**: Tuned for Perlmutter architecture with SLURM integration
- **Configuration Driven**: YAML-based configuration for easy parameter management
- **Flexible Date/Time**: Support for daily, hourly, and sub-hourly time specifications
- **Testing**: Built-in validation and test suite with pattern testing tools

## Quick Start

### 1. Environment Setup
```bash
# On NERSC Perlmutter
source activate /global/common/software/m1867/python/hackathon
cd /path/to/healpix-remapping
```

### 2. Test Installation
```bash
python tests/test_setup.py
```

### 3. Configure for Your Dataset

**For IMERG dataset:** Edit `config/imerg_config.yaml`  
**For IR_IMERG dataset:** Edit `config/tb_imerg_config.yaml`  
**For other datasets:** Copy and customize one of the above configs

#### Key Configuration Sections:

**a) Data Paths:**
```yaml
input_base_dir: "/path/to/your/input/data/"
output_base_dir: "/path/to/output/zarr/files/"
output_basename: "MyDataset_V1"
weights_dir: "/path/to/weights/"  # Weights are cached and reused
```

**b) File Pattern Matching:**
```yaml
# Example for IMERG files: 3B-HHR.MS.MRG.3IMERG.20211231-S170000-E172959.1020.V07B.HDF5.nc4
date_pattern: "\\.(\\d{8})-"              # Regex to extract date (use \\d for digits)
date_format: "%Y%m%d"                     # How to parse the extracted date
use_year_subdirs: true                    # Files in YYYY/ subdirectories?
file_glob: "3B-HHR.MS.MRG.3IMERG.*.nc4"  # Glob to match files

# Example for IR_IMERG files: merg_2020123108_10km-pixel.nc
date_pattern: "_(\\d{10})_"               # Extract YYYYMMDDhh
date_format: "%Y%m%d%H"                   # Parse with hour
use_year_subdirs: true                    # Files in YYYY/ subdirectories
file_glob: "merg_*.nc"                    # Match merg files
```

**c) Processing Parameters:**
```yaml
default_zoom: 9                # HEALPix zoom level (9 recommended for ~0.1° data)
time_chunk_size: 24           # Number of time steps per chunk (balance memory/parallelism)
time_average: "1h"            # Temporal averaging: null, "1h", "3h", "6h", "1d"
convert_time: True            # Convert cftime to datetime64 for pandas

# Spatial dimension configuration (OPTIONAL - will auto-detect if not specified)
# spatial_dimensions:         # Override auto-detection for explicit control
#   lat: -1                   # -1 means no chunking (keep full dimension)
#   lon: -1
# 
# Common spatial dimension names by dataset:
#   IMERG/IR_IMERG: {lat: -1, lon: -1}
#   ERA5: {latitude: -1, longitude: -1}
#   E3SM: {ncol: -1}
#   CMIP6: varies by model - may be lat/lon or latitude/longitude
#
# Note: Auto-detection examines the first data file and identifies coordinate
# dimensions automatically. Only use explicit config if auto-detection fails
# or you need to override for a specific reason.

# Time dimension configuration (OPTIONAL - defaults to 'time')
# concat_dim: "time"          # Name of time/concatenation dimension
#
# Common time dimension names:
#   Most datasets: "time" (default)
#   WRF: "Time" or "Times"
#   Some CMIP6: "time_counter"
#
# Only specify if your dataset uses a different name than 'time'
```

**d) Dask Configuration (Optimized for I/O-Intensive Operations):**
```yaml
dask:
  n_workers: 16              # More workers = better parallel I/O throughput
  threads_per_worker: 1      # Single thread avoids GIL contention during I/O
  # memory_limit: auto        # Auto-calculated: 80% of system RAM / n_workers
```

**Why these settings work:**
- **More workers (16) with single thread** is optimal for I/O-bound zarr writes
- **Auto-calculated memory** prevents worker pausing/deadlocks (uses psutil)
- **Simplified configuration** avoids timeout conflicts that cause connection resets
- **LocalCluster pattern** (in utilities.py) ensures memory limits are properly respected

**Performance validation:**
- ✅ Successfully processed 3 years (26,304 files) in 40.9 minutes
- ✅ Write throughput: 21.2 GB/minute
- ✅ No deadlocks or OOM issues
- ✅ Scales linearly (or better) with data size

### 4. Test Your File Pattern Configuration

**Before processing, always test your file pattern:**

```bash
cd tests
python test_file_pattern.py
```

This will:
- ✅ Show how many files match your pattern
- ✅ Display sample filenames found
- ✅ Verify date extraction works correctly
- ✅ Confirm files fall within your date range

If no files are found or dates aren't parsed correctly, adjust your config and test again.

### 5. Process Your Data

**Single day:**
```bash
cd scripts
python launch_imerg_processing.py 2020-01-01 2020-01-01 9
```

**Single month:**
```bash
python launch_imerg_processing.py 2020-01-01 2020-01-31 9
```

**With hourly precision:**
```bash
python launch_imerg_processing.py "2020-01-01T06" "2020-01-01T18" 9
```

**Full year (batch job):**
```bash
sbatch submit_imerg_job.sh 2020-01-01 2020-12-31 9
```

**With overwrite option:**
```bash
python launch_imerg_processing.py 2020-01-01 2020-01-31 9 --overwrite
```

## Project Structure

```
healpix-remapping/
├── README.md                           # This file
├── remap_to_healpix.py             # Main library module
├── setup.sh                           # Environment setup script
│
├── config/                            # Configuration files
│   ├── imerg_config.yaml             # IMERG dataset configuration
│   └── tb_imerg_config.yaml          # IR_IMERG dataset configuration
│
├── scripts/                           # Execution scripts
│   ├── launch_imerg_processing.py    # IMERG launcher
│   ├── launch_ir_imerg_processing.py # IR_IMERG launcher
│   ├── coarsen_healpix.py            # HEALPix coarsening utility
│   └── example_usage.py              # 4 dataset examples
│
├── tests/                             # Test utilities
│   ├── test_setup.py                 # Environment validation
│   └── test_file_pattern.py          # File pattern testing (USE THIS FIRST!)
│
├── src/                               # Library modules
│   ├── __init__.py                   # Package initialization
│   ├── utilities.py                  # General utilities
│   ├── zarr_tools.py                 # Zarr I/O utilities
│   ├── remap_tools.py                # Remapping functions
│   └── chunk_tools.py                # Chunking calculations
│
└── notebooks/                         # Jupyter notebooks for development
```


## Configuring for Your Dataset

### Step-by-Step Configuration Guide

#### 1. Understanding File Pattern Matching

The workflow needs to know:
- **Where to find your files** (`input_base_dir`)
- **What files to look for** (`file_glob`)
- **How to extract dates from filenames** (`date_pattern` + `date_format`)
- **How files are organized** (`use_year_subdirs`)

#### 2. Determine Your File Pattern Parameters

**Example 1: IMERG Format**
```
Filename: 3B-HHR.MS.MRG.3IMERG.20211231-S170000-E172959.1020.V07B.HDF5.nc4
Directory: /data/IMERG/2021/3B-HHR.MS.MRG.3IMERG.20211231-S170000-E172959.1020.V07B.HDF5.nc4
```

Configuration:
```yaml
date_pattern: "\\.(\\d{8})-"              # Match .20211231- and extract 20211231
date_format: "%Y%m%d"                     # Parse as YYYYMMDD
use_year_subdirs: true                    # Files in year folders (2021/)
file_glob: "3B-HHR.MS.MRG.3IMERG.*.nc4"  # Match IMERG HDF5 files
```

**Example 2: IR_IMERG Format**
```
Filename: merg_2020123108_10km-pixel.nc
Directory: /data/IR_IMERG/2020/merg_2020123108_10km-pixel.nc
```

Configuration:
```yaml
date_pattern: "_(\\d{10})_"    # Match _2020123108_ and extract 2020123108
date_format: "%Y%m%d%H"        # Parse as YYYYMMDDhh (includes hour!)
use_year_subdirs: true         # Files in year folders
file_glob: "merg_*.nc"         # Match merg files
```

**Example 3: Generic Daily Files (Flat Directory)**
```
Filename: precipitation_2020-01-31.nc
Directory: /data/precip/precipitation_2020-01-31.nc  (no year subdirs)
```

Configuration:
```yaml
date_pattern: "_(\\d{4}-\\d{2}-\\d{2})"  # Match _2020-01-31 and extract it
date_format: "%Y-%m-%d"                  # Parse with dashes
use_year_subdirs: false                  # All files in one directory
file_glob: "precipitation_*.nc"          # Match precipitation files
```

#### 3. Test Your Configuration

**Before processing data, always test your file pattern:**

```bash
cd tests
python test_file_pattern.py
```

Edit the test script to use your config file:
```python
# In test_file_pattern.py, modify:
config_file = "../config/your_config.yaml"  # Point to your config
```

The test will show:
- ✅ Total files matching your pattern
- ✅ Sample filenames found
- ✅ Extracted dates
- ✅ Files within your specified date range

**Common Issues:**
- **"No files found"**: Check `file_glob` pattern and `input_base_dir` path
- **"Could not parse date"**: Check that `date_pattern` has parentheses `(...)` and matches your filename
- **Wrong date extracted**: Verify `date_format` matches the digits extracted by `date_pattern`
- **Missing files**: Check `use_year_subdirs` setting matches your directory structure

#### 4. Understanding Date Pattern Regex

The `date_pattern` is a **regular expression** that extracts the date string from your filename:

- `\\d` = any digit (0-9)
- `{8}` = exactly 8 characters
- `()` = **REQUIRED** - captures the date string
- `\\.` = literal dot (escaped)
- `_` = literal underscore

**Common patterns:**
```python
# Extract 8 digits: 20200101
"(\\d{8})"           # Anywhere: data20200101.nc

# Extract 8 digits after dot: .20200101-
"\\.(\\d{8})-"       # IMERG format

# Extract 10 digits between underscores: _2020010100_
"_(\\d{10})_"        # IR_IMERG with hour

# Extract date with dashes: 2020-01-01
"(\\d{4}-\\d{2}-\\d{2})"  # ISO format

# Extract date between dot and dot: .20200101.
"\\.(\\d{8})\\."     # data.20200101.v2.nc
```

#### 5. Date/Time Specification Flexibility

When running scripts, you can specify dates in multiple formats:

**Daily (extends to end of day automatically):**
```bash
python scripts/launch_imerg_processing.py 2020-01-01 2020-01-03 9
# Processes: 2020-01-01 00:00:00 through 2020-01-03 23:59:59
```

**Hourly (extends to end of hour automatically):**
```bash
python scripts/launch_imerg_processing.py 2020-01-01T06 2020-01-01T18 9
# Processes: 2020-01-01 06:00:00 through 2020-01-01 18:59:59
```

**With spaces (quote the date):**
```bash
python scripts/launch_imerg_processing.py "2020-01-01 06" "2020-01-01 18" 9
# Same as above
```

This means:
- ✅ "2020-01-03" automatically includes all hours of Jan 3rd
- ✅ "2020-01-01T12" automatically includes the entire 12:00 hour
- ✅ No need to manually specify "23:59:59" for end dates!

## Processing Parameters & Performance

### HEALPix Zoom Levels
- **Zoom 7**: 196,608 cells (~0.4° resolution)
- **Zoom 8**: 786,432 cells (~0.2° resolution) 
- **Zoom 9**: 3,145,728 cells (~0.1° resolution) - **Recommended for ~0.1° input data**

### Chunking Strategy
- **Time chunks**: Configurable via `time_chunk_size` (default: 24 time steps)
- **Spatial chunks**: Computed automatically per zoom level
  - Zoom 9: 262,144 cells/chunk (12 spatial chunks total)

### Temporal Averaging Options

Control output temporal resolution with the `time_average` parameter:

```yaml
time_average: null    # No averaging - keep original resolution
time_average: "1h"    # Average to 1-hour (e.g., 30-min → 1-hour)
time_average: "3h"    # Average to 3-hour
time_average: "6h"    # Average to 6-hour  
time_average: "1d"    # Average to daily
```

**Benefits:**
- Reduces output file size
- Smooths high-frequency noise
- Matches typical analysis timescales

### Performance Metrics (Validated on NERSC Perlmutter)

**Configuration:** 16 workers × 1 thread, auto-calculated memory (~23-25GB per worker)

**IR_IMERG 1-Year Processing (Zoom 9, no averaging):**
- Input: 8,784 hourly files
- Output: 205.9 GB compressed Zarr
- Runtime: 15.5 minutes
- Throughput: 17.7 GB/minute write speed
- Processing rate: 568 files/minute

**IR_IMERG 3-Year Processing (Zoom 9, no averaging):**
- Input: 26,304 hourly files  
- Output: 616.5 GB compressed Zarr
- Runtime: 40.9 minutes
- Throughput: 21.2 GB/minute write speed (20% faster than 1-year!)
- Processing rate: 644 files/minute

**Key insights:**
- ✅ **Scales better than linearly** - Larger datasets provide better parallelism
- ✅ **Stable memory usage** - Workers at ~79-80% with brief acceptable pauses
- ✅ **No deadlocks** - Simplified Dask config avoids timeout conflicts
- ✅ **Predictable runtime** - ~13.6 min/year for IR_IMERG processing

**Memory & Chunking:**
- Memory: ~24MB per chunk (24 time steps × 262K cells × 4 bytes)
- Zoom 9: 262,144 cells/chunk (12 spatial chunks total)
- Time chunks: 24 hours (configurable via `time_chunk_size`)
- Temporal averaging: 30-minute → 1-hour reduces output by 50%

## Examples & Usage

### See Complete Examples

Check `scripts/example_usage.py` for 4 complete working examples:
1. **IMERG format** - Original IMERG HDF5 files
2. **IR_IMERG format** - Merged IR-IMERG files with hourly timestamps
3. **Generic format** - Files with ISO date format (YYYY-MM-DD)
4. **Timestamp suffix** - Files with date suffix (.YYYYMMDD.)

### Process a Single Month
```bash
cd scripts
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
- **Naming**: `{output_basename}_zoom{zoom}_{start_date}_{end_date}.zarr`
- **Structure**: Time series of HEALPix-remapped data
- **Compression**: zstd level 3 for optimal size/speed balance (configurable)
- **Temporal resolution**: Original or averaged (configurable via `time_average`)
- **Chunks**: Optimized for subsequent analysis workflows

**Zarr Structure:**
```
output.zarr/
├── precipitation/        # Main data variable (or your variable name)
├── time/                # Time coordinates (datetime64[ns])
├── cell/                # HEALPix cell indices
├── crs/                 # Grid mapping metadata
└── .zmetadata           # Consolidated metadata
```

**Metadata includes:**
- HEALPix parameters (nside, order, nest=True)
- Original grid information
- Processing timestamps
- Temporal averaging information (if applied)
- Chunk configuration
- Compression settings


## Post-Processing Scripts

### coarsen_healpix.py

Located in `scripts/coarsen_healpix.py`, this utility performs spatial and temporal coarsening of HEALPix datasets stored in Zarr format. It works with any HEALPix data, not limited to specific datasets.

#### Features

- **Spatial Coarsening**: Reduces HEALPix resolution by aggregating data to a lower zoom level
- **Temporal Coarsening**: Two methods available:
  - `--temporal_factor`: Simple integer downsampling (e.g., factor=6 averages every 6 time steps)
  - `--target_hours`: Flexible resampling to specific hour alignments (e.g., [0, 6, 12, 18] for 6-hourly data)
- **Flexible I/O**: Works with any HEALPix Zarr dataset structure
- **Optional Configuration**: Supports custom compression settings via YAML config file
- **Chunked Processing**: Memory-efficient processing with configurable time chunk sizes

#### Usage Examples

**Basic spatial coarsening** (reduce resolution):
```bash
python scripts/coarsen_healpix.py \
    /path/to/input.zarr \
    --output_dir /path/to/output \
    --target_zoom 4
```

**Temporal averaging with target hours** (resample to 6-hourly at specific times):
```bash
python scripts/coarsen_healpix.py \
    /path/to/input.zarr \
    --output_dir /path/to/output \
    --target_zoom 4 \
    --target_hours 0 6 12 18
```

**Simple temporal downsampling** (average every 6 time steps):
```bash
python scripts/coarsen_healpix.py \
    /path/to/input.zarr \
    --output_dir /path/to/output \
    --target_zoom 4 \
    --temporal_factor 6
```

**With custom configuration**:
```bash
python scripts/coarsen_healpix.py \
    /path/to/input.zarr \
    --output_dir /path/to/output \
    --target_zoom 4 \
    --config config/my_compression_config.yaml
```

#### When to Use Each Temporal Method

- Use `--temporal_factor` when your input data has regular time intervals and you want simple downsampling
- Use `--target_hours` when you need output aligned to specific hours of the day, regardless of input time structure (e.g., converting 3-hourly data at hours [1, 4, 7, 10...] to 6-hourly at [0, 6, 12, 18])

For complete usage information, run:
```bash
python scripts/coarsen_healpix.py --help
```

## Troubleshooting

### General Issues

**Memory errors:**
- Reduce `time_chunk_size` in config (try 12 or 6 instead of 24)
- Reduce `n_workers` in Dask config
- Increase `memory_limit` per worker if you have available RAM

**Slow I/O:**
- Check storage system performance (use `df -h` and `iostat`)
- Ensure input/output paths are on fast scratch filesystem
- Avoid reading/writing across network mounts

**Missing dependencies:**
- Run `python tests/test_setup.py` to validate environment
- Check Python version (requires 3.10+)
- Verify all required packages are installed

**File path errors:**
- Check for double slashes in output paths
- Ensure directories exist or have write permissions
- Use absolute paths to avoid confusion

### File Pattern Issues

**"No files found for date range":**
1. Test with `python tests/test_file_pattern.py` first
2. Check `file_glob` pattern matches your files
3. Verify `input_base_dir` path is correct
4. Confirm `use_year_subdirs` matches your directory structure
5. Check that date range actually contains files

**"Could not parse date from filename":**
1. Ensure `date_pattern` has parentheses `(...)` to capture the date
2. Verify the regex matches your filename format
3. Test regex with sample filename using Python's `re` module
4. Check that `date_format` matches the captured string

**Wrong files selected / dates extracted incorrectly:**
1. Print extracted dates with test script to verify
2. Ensure `date_format` exactly matches the date string format
3. For hourly data, include `%H` in format string
4. Check that regex is specific enough (not matching multiple patterns)

**Example debugging in Python:**
```python
import re
filename = "merg_2020123108_10km-pixel.nc"
pattern = r"_(\d{10})_"
match = re.search(pattern, filename)
if match:
    print(f"Extracted: {match.group(1)}")  # Should print: 2020123108
    from datetime import datetime
    date = datetime.strptime(match.group(1), "%Y%m%d%H")
    print(f"Parsed date: {date}")  # Should print: 2020-12-31 08:00:00
```

### Performance Issues

**Workers hitting 80% memory and pausing (deadlock):**
- ✅ **Solution**: Comment out `memory_limit` in config to use auto-calculation
- ✅ **Solution**: Use LocalCluster pattern (not bare Client)
- Check: Worker memory limits should be ~23-25GB with 16 workers on 512GB node

**Connection reset errors / Worker killed by signal 9:**
- ✅ **Solution**: Remove all advanced timeout/communication/scheduler settings
- ✅ **Solution**: Use simplified Dask config (see `config/tb_imerg_config.yaml`)
- The issue: Complex timeout settings cause conflicts during zarr writes

**Zarr write stuck at same percentage for >10 minutes:**
- ✅ **Solution**: Reduce threads per worker from 8→1 (avoids GIL contention)
- ✅ **Solution**: Increase workers from 4→16 (more parallel I/O)
- Check Dask dashboard for worker status and task distribution

**Processing slower than expected:**
- Check if you're reading from slow storage (use `/pscratch` on NERSC)
- Verify workers are actually using allocated resources (check dashboard)
- Expected: ~13-14 min/year for hourly datasets at zoom 9

**Large output files:**
- Enable temporal averaging: `time_average: "1h"` (reduces output by 50%)
- Check compression settings: `compressor: "zstd"`, `compressor_level: 3`
- Verify time subsetting if using duplicate timestamps (e.g., IR_IMERG)

### Getting Help

1. **Check the test utilities:**
   - `python tests/test_setup.py` - Environment validation
   - `python tests/test_file_pattern.py` - File pattern testing

2. **Review the examples:**
   - `scripts/example_usage.py` - Working examples for 4 dataset formats
   - Compare your config to the provided examples

3. **Check logs:**
   - SLURM output files contain full error messages
   - Look for Python tracebacks with line numbers
   - Dask errors often indicate memory or I/O issues

4. **Verify configuration:**
   - Review your YAML config file carefully
   - Compare against `config/imerg_config.yaml` or `config/tb_imerg_config.yaml`
   - Ensure all paths use absolute paths or correct relative paths

## Performance Tips & Best Practices

1. **Test first**: Always run `tests/test_file_pattern.py` before processing
2. **Start small**: Process 1-3 days first to verify everything works
3. **Use optimized Dask settings**:
   - ✅ 16 workers × 1 thread (optimal for I/O-bound zarr writes)
   - ✅ Auto-calculated memory (prevents worker pausing/deadlocks)
   - ❌ Avoid complex timeout/communication settings (causes connection resets)
4. **Memory**: Use `time_chunk_size=24` for daily chunks with hourly data
5. **I/O**: Use fast scratch storage (`/pscratch` on NERSC) for both input and output
6. **Caching**: Weight files are cached - reuse across processing runs
7. **Temporal averaging**: Use `"1h"` or `"3h"` to reduce output size by 50%+
8. **Monitoring**: Check Dask dashboard at `http://localhost:8787` during processing
9. **Scaling**: Expect ~13-14 min/year processing time for hourly datasets at zoom 9
10. **LocalCluster**: Always use `LocalCluster() + Client(cluster)` pattern (not bare `Client()`)

### Dask Configuration Guidelines

**✅ DO:**
- Use 16 workers with 1 thread per worker for I/O-intensive operations
- Let Dask auto-calculate memory limits (80% system RAM / n_workers)
- Use LocalCluster pattern for proper memory control
- Keep configuration simple - Dask defaults work well

**❌ DON'T:**
- Use many threads per worker (causes GIL contention during I/O)
- Manually set aggressive timeout values (causes connection resets)
- Use bare `Client()` without LocalCluster (loses memory control)
- Add complex communication/scheduler settings (increases deadlock risk)


## Further Development

### Extending for Different Input Grid Types

The current workflow is optimized for **regular 1D lat/lon gridded data** (e.g., IMERG, where lat and lon are 1D arrays). The code can be adapted for other grid types with modifications to `src/remap_tools.py` → `gen_weights()` function.

**File to modify:** `src/remap_tools.py` → `gen_weights()` function

#### Current Implementation for 1D Lat/Lon Grids

The `gen_weights()` function currently handles regular 1D lat/lon grids by creating a 2D meshgrid:

```python
# Create 2D meshgrid and flatten for regular lat/lon grid
lon_2d, lat_2d = np.meshgrid(ds.lon.values, ds.lat.values)
source_lon = lon_2d.flatten()
source_lat = lat_2d.flatten()
```

This creates a 2D grid from 1D coordinate arrays, then flattens it for Delaunay triangulation.

#### For Unstructured Grid Data (e.g., Global Climate Models)

**Good news:** Unstructured grids are actually **simpler** to handle! Each cell already has unique lat/lon coordinates, so the meshgrid step is unnecessary.

**Example modification for unstructured grids:**

```python
# For unstructured grids (e.g., SCREAM, E3SM with ncol dimension)
# Coordinates are already 1D arrays with one value per cell
source_lon = ds.lon.values  # Already 1D: (ncol,)
source_lat = ds.lat.values  # Already 1D: (ncol,)

# For global unstructured grids, handle periodicity:
lon_periodic = np.hstack((source_lon - 360, source_lon, source_lon + 360))
lat_periodic = np.hstack((source_lat, source_lat, source_lat))

# Compute weights with periodic extension
hp_lon, hp_lat = hp.pix2ang(nside=nside, ipix=np.arange(npix), lonlat=True, nest=True)
hp_lon = (hp_lon + 180) % 360 - 180  # Shift to [-180, 180)
hp_lon += 360 / (4 * nside) / 4      # Quarter-width shift

weights = egr.compute_weights_delaunay(
    points=(lon_periodic, lat_periodic),
    xi=(hp_lon, hp_lat)
)

# Remap source indices back to valid range
weights = weights.assign(src_idx=weights.src_idx % source_lat.size)
```

#### For 2D Lat/Lon Grids (e.g., WRF)

If your input data already has 2D lat/lon arrays (e.g., WRF model output with `XLAT(y, x)` and `XLONG(y, x)`), the meshgrid step is also unnecessary:

```python
# For 2D lat/lon grids (e.g., WRF output)
# Coordinates are already 2D: (nlat, nlon)
source_lon = ds.lon.values.flatten()
source_lat = ds.lat.values.flatten()

# Then proceed with weight computation as usual
```

#### Recommended Enhancement: Add Grid Type Detection

Consider adding a grid type flag or automatic detection to `gen_weights()`:

```python
def gen_weights(ds, order, weights_file=None, force_recompute=False, grid_type='auto'):
    """
    Parameters:
    -----------
    grid_type : str, default='auto'
        Type of input grid:
        - 'auto': Automatically detect from coordinate dimensions
        - 'latlon_1d': Regular lat/lon with 1D coordinates (requires meshgrid)
        - 'latlon_2d': Curvilinear grid with 2D lat/lon arrays
        - 'unstructured': Unstructured grid with 1D coordinate per cell
    """
    
    # Auto-detect grid type
    if grid_type == 'auto':
        if ds.lon.ndim == 1 and ds.lat.ndim == 1:
            if 'ncol' in ds.dims or 'cell' in ds.dims:
                grid_type = 'unstructured'
            else:
                grid_type = 'latlon_1d'
        elif ds.lon.ndim == 2 and ds.lat.ndim == 2:
            grid_type = 'latlon_2d'
    
    # Handle different grid types
    if grid_type == 'latlon_1d':
        # Current implementation: create meshgrid
        lon_2d, lat_2d = np.meshgrid(ds.lon.values, ds.lat.values)
        source_lon = lon_2d.flatten()
        source_lat = lat_2d.flatten()
    
    elif grid_type == 'latlon_2d':
        # 2D coordinates: just flatten
        source_lon = ds.lon.values.flatten()
        source_lat = ds.lat.values.flatten()
    
    elif grid_type == 'unstructured':
        # Unstructured: coordinates already 1D per cell
        source_lon = ds.lon.values
        source_lat = ds.lat.values
    
    # Continue with weight computation...
```

**Note:** The current implementation uses `easygems.remap` for Delaunay triangulation, not ESMF. This approach works well for all grid types mentioned above.

### Splitting Output into Multiple Zarr Files

The current workflow combines **all input files within the date range** into a **single Zarr output file**. This design choice optimizes for:
- Single time-series analysis
- Reduced file management overhead
- Efficient time-dimension chunking

**If you need separate output files** (e.g., one Zarr file per day, month, or year), you'll need to modify the main processing logic:

**File to modify:** `remap_to_healpix.py` → Main processing loop

**Approach 1: Multiple time ranges**
```python
from datetime import datetime
import calendar
from remap_to_healpix import process_to_healpix_zarr

# Instead of one large date range, process month by month
for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        monthly_start = datetime(year, month, 1)
        monthly_end = datetime(year, month, calendar.monthrange(year, month)[1])
        output_file = f"{output_base}/data_{year}_{month:02d}.zarr"
        
        process_to_healpix_zarr(
            start_date=monthly_start,
            end_date=monthly_end,
            zoom=9,
            output_zarr=output_file,
            input_base_dir="/data/input",
            # ... other parameters ...
        )
```

**Approach 2: Modify internal chunking**
- Split the time dimension processing in `utilities.read_concat_files()` and `process_to_healpix_zarr()`
- Write separate Zarr files for each temporal chunk
- Update the output file naming logic

**Trade-offs to consider:**
- ✅ More granular outputs easier to manage individually
- ✅ Can process subsets independently
- ❌ More files to track and open for analysis
- ❌ Potential overhead from repeated metadata/coordinate writes
- ❌ Cross-file time-series queries more complex

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
