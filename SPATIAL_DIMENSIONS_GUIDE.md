# Spatial Dimensions Configuration Guide

This guide explains how spatial dimension names are handled in the HEALPix remapping pipeline, along with time dimension naming.

## Overview

Different datasets use different names for their spatial coordinate dimensions and time dimensions. The pipeline uses a **hybrid approach** to handle this gracefully:

1. **Auto-detection** (default): Automatically detect dimension names from the first data file
2. **Config override**: Explicitly specify dimension names in your config file
3. **Fallback**: Use sensible defaults if detection fails

## Time Dimension Names

The pipeline also handles different time dimension names across datasets:

### Common Time Dimension Names

| Dataset | Time Dimension | Config Example |
|---------|---------------|----------------|
| **IMERG/IR_IMERG** | `time` | `concat_dim: "time"` (default) |
| **ERA5** | `time` | `concat_dim: "time"` (default) |
| **WRF** | `Times` | `concat_dim: "Times"` |
| **CESM/CAM** | `time` | `concat_dim: "time"` (default) |
| **Some CMIP6** | `time_counter` | `concat_dim: "time_counter"` |

### Configuring Time Dimension

**In config file** (`config/tb_imerg_config.yaml`):
```yaml
# Time dimension configuration (OPTIONAL)
# Only specify if your dataset uses a different name (default: 'time')
concat_dim: "Time"  # For WRF data
```

**Why this matters**: The auto-detection algorithm needs to know which dimension is time so it can correctly identify spatial dimensions (everything except time).

## Why This Matters

The pipeline needs to know which dimensions are spatial (not time) to:
- Apply proper chunking strategy (typically `-1` = no chunking for remapping)
- Build the correct chunks dictionary for `xr.open_mfdataset()`
- Handle diverse dataset formats without hardcoding

### Chunking Strategy

The value `-1` means **"do not chunk this dimension"** - keep the full dimension in a single Dask chunk. This is optimal for HEALPix remapping because:

- Delaunay triangulation requires access to full spatial grids
- Chunked spatial data would require expensive rechunking before remapping
- Time dimension uses positive values (e.g., `48`) to enable parallel processing

Example chunks dictionary: `{'time': 48, 'lat': -1, 'lon': -1}`
- Time chunked into 48-timestep pieces → parallelism
- Lat/lon kept whole → efficient remapping

## Common Dataset Dimension Names

### Regular Grids (Lat/Lon)

| Dataset | Dimensions | Example Config |
|---------|-----------|----------------|
| **IMERG** | `lat`, `lon` | `{lat: -1, lon: -1}` |
| **IR_IMERG** | `lat`, `lon` | `{lat: -1, lon: -1}` |
| **ERA5** | `latitude`, `longitude` | `{latitude: -1, longitude: -1}` |
| **MERRA-2** | `lat`, `lon` | `{lat: -1, lon: -1}` |
| **CMIP6** | varies | `{lat: -1, lon: -1}` or `{latitude: -1, longitude: -1}` |

### Unstructured Grids

| Dataset | Dimensions | Example Config |
|---------|-----------|----------------|
| **E3SM** | `ncol` | `{ncol: -1}` |
| **MPAS** | `nCells` | `{nCells: -1}` |
| **ICON** | `cell` | `{cell: -1}` |

### Multi-Dimensional Coordinates

| Dataset | Dimensions | Example Config |
|---------|-----------|----------------|
| **WRF** | `south_north`, `west_east` | `{south_north: -1, west_east: -1}` |
| **RegCM** | `iy`, `jx` | `{iy: -1, jx: -1}` |

## How Auto-Detection Works

The `detect_spatial_dimensions()` function in `src/utilities.py`:

1. Opens the first file in your file list
2. Examines all coordinate variables
3. Identifies 1D coordinates that are not the time dimension
4. Returns a dictionary mapping dimension names to `-1`

**Example detection logic:**
```python
# For IMERG file with coordinates: time, lat, lon
# Result: {'lat': -1, 'lon': -1}

# For ERA5 file with coordinates: time, latitude, longitude
# Result: {'latitude': -1, 'longitude': -1}

# For E3SM file with coordinates: time, ncol
# Result: {'ncol': -1}
```

### Detection Priorities

1. **1D Coordinates**: Identifies coordinate variables with single dimension (standard approach)
2. **Common Names**: Fallback to checking common dimension names (lat, lon, latitude, longitude, ncol, cell, etc.)
3. **All Non-Time**: Last resort - use all dimensions except 'time'
4. **Ultimate Fallback**: Returns `{'lat': -1, 'lon': -1}` if everything fails

## Configuration Options

### Option 1: Let Auto-Detection Work (Recommended)

**When to use:**
- Standard dataset formats
- Dimension names follow conventions
- You want automatic handling

**How to use:**
Simply omit `spatial_dimensions` from your config file (or comment it out):

```yaml
# config/my_dataset_config.yaml
input_base_dir: "/path/to/data"
output_base_dir: "/path/to/output"
default_zoom: 9
# spatial_dimensions: not specified - will auto-detect
```

**Output during processing:**
```
🔍 Spatial dimensions not specified - auto-detecting from first file...
Auto-detected spatial dimensions: ['lat', 'lon']
```

### Option 2: Explicit Configuration (Override)

**When to use:**
- Non-standard dimension names
- Auto-detection fails or is incorrect
- You need explicit control
- Dataset has ambiguous coordinates

**How to use:**
Add `spatial_dimensions` to your config file:

```yaml
# config/my_dataset_config.yaml
input_base_dir: "/path/to/data"
output_base_dir: "/path/to/output"
default_zoom: 9

# Explicit spatial dimension specification
spatial_dimensions:
  latitude: -1
  longitude: -1
```

**Output during processing:**
```
✅ Using explicitly provided spatial dimensions: {'latitude': -1, 'longitude': -1}
```

### Option 3: Hybrid Approach (Best of Both Worlds)

The pipeline naturally supports a hybrid approach:
- Most datasets → auto-detect (no config needed)
- Special cases → explicit config (when needed)

This means you can use auto-detection as default and only specify dimensions when necessary.

## Examples by Dataset

### IMERG Precipitation

```yaml
# config/imerg_config.yaml
# Auto-detection works perfectly - no spatial_dimensions needed
input_base_dir: "/data/IMERG"
default_zoom: 9
time_average: "1h"
```

Auto-detects: `{'lat': -1, 'lon': -1}` ✅

### ERA5 Reanalysis

```yaml
# config/era5_config.yaml
# Can use auto-detection OR explicit config
input_base_dir: "/data/ERA5"
default_zoom: 9

# Option A: Auto-detect (recommended)
# (no spatial_dimensions specified)

# Option B: Explicit override
# spatial_dimensions:
#   latitude: -1
#   longitude: -1
```

Auto-detects: `{'latitude': -1, 'longitude': -1}` ✅

### E3SM Simulation (Unstructured Grid)

```yaml
# config/e3sm_config.yaml
# Auto-detection handles unstructured grids
input_base_dir: "/data/E3SM"
default_zoom: 9

# No spatial_dimensions needed - auto-detection works
```

Auto-detects: `{'ncol': -1}` ✅

### WRF Model Output

```yaml
# config/wrf_config.yaml
# May need explicit config depending on coordinate setup
input_base_dir: "/data/WRF"
default_zoom: 9

# WRF uses 'Time' or 'Times' instead of 'time'
concat_dim: "Times"

# Explicit config recommended for WRF
spatial_dimensions:
  south_north: -1
  west_east: -1
```

Explicit: `{'south_north': -1, 'west_east': -1}` ✅  
Time dimension: `'Times'` ✅

## Troubleshooting

### Problem: Auto-Detection Fails

**Symptoms:**
```
Failed to auto-detect spatial dimensions: <error message>
Using default spatial dimensions: {'lat': -1, 'lon': -1}
```

**Solutions:**
1. Check if the first file in your date range is corrupted or incomplete
2. Add explicit `spatial_dimensions` to your config file
3. Verify your file pattern configuration (might be reading wrong files)

### Problem: Wrong Dimensions Detected

**Symptoms:**
```
Auto-detected spatial dimensions: ['x', 'y']  # But you expected lat/lon
```

**Solutions:**
1. Add explicit `spatial_dimensions` to override auto-detection
2. Check if coordinates are properly defined in your NetCDF files
3. Verify the first file is representative of your dataset

### Problem: Missing Spatial Dimensions in Config

**Symptoms:**
```
KeyError: 'lat' not found in dataset dimensions
```

**Solutions:**
1. Your config specifies `{lat: -1, lon: -1}` but dataset uses different names
2. Remove `spatial_dimensions` from config to enable auto-detection
3. Or update config with correct dimension names

### Problem: Performance Issues with Large Grids

**Symptoms:**
- Memory errors during processing
- Very slow remapping step

**Solutions:**
Consider chunking very large spatial grids:

```yaml
spatial_dimensions:
  lat: 500    # Chunk to 500 grid points
  lon: 500
```

**Note:** This adds overhead for rechunking before remapping. Only use if memory constrained.

## Advanced Usage

### Custom Spatial Chunking

For exceptionally large grids (e.g., 0.01° global), you may need spatial chunking:

```yaml
# For 36000 x 18000 grid
spatial_dimensions:
  lat: 1000
  lon: 1000
```

This breaks the grid into 1000x1000 chunks. The remapping step will rechunk as needed, but memory usage is reduced during file reading.

### Multiple Spatial Dimensions (Unstructured + Regular)

Some datasets have multiple spatial dimensions:

```yaml
# Hybrid grid
spatial_dimensions:
  ncol: -1      # Unstructured dimension
  level: -1     # Vertical levels
```

### Programmatic Access

In Python scripts, you can pass spatial dimensions directly:

```python
from remap_to_healpix import process_to_healpix_zarr
from datetime import datetime

# Let auto-detection work
process_to_healpix_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    zoom=9,
    output_zarr="/output/data.zarr",
    input_base_dir="/input/data",
    spatial_chunks=None  # Auto-detect
)

# Or explicit override
process_to_healpix_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    zoom=9,
    output_zarr="/output/data.zarr",
    input_base_dir="/input/data",
    spatial_chunks={'latitude': -1, 'longitude': -1}  # Explicit
)
```

## Summary

- **Default**: Auto-detection handles most datasets automatically
- **Override**: Add `spatial_dimensions` to config when needed
- **Chunking**: Use `-1` for no chunking (optimal for remapping)
- **Hybrid**: Best of both worlds - auto-detect by default, override when necessary

The hybrid approach makes the pipeline both user-friendly and flexible, supporting diverse dataset formats without manual configuration in most cases.
