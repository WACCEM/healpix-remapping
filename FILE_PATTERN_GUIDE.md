# File Pattern Configuration Guide

This guide explains how to configure the `process_imerg_to_zarr` function to work with different filename patterns and directory structures.

## Quick Reference

### Key Parameters

| Parameter | Purpose | Example |
|-----------|---------|---------|
| `date_pattern` | Regex to extract date string from filename | `r'_(\d{10})_'` |
| `date_format` | strptime format to parse the date | `'%Y%m%d%H'` |
| `use_year_subdirs` | Whether files are organized in yearly folders | `True` or `False` |
| `file_glob` | Glob pattern to match files | `'*.nc'` |

## Common Filename Patterns

### 1. IMERG Format (Original)
**Filename:** `3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4`

```python
date_pattern = r'\.(\d{8})-'     # Matches .20200101-
date_format = '%Y%m%d'           # YYYYMMDD
use_year_subdirs = True          # Files in /YYYY/ folders
file_glob = '*.nc*'              # Match .nc, .nc4, etc.
```

### 2. ir_imerg Format  
**Filename:** `merg_2020123108_10km-pixel.nc`

```python
date_pattern = r'_(\d{10})_'     # Matches _2020123108_
date_format = '%Y%m%d%H'         # YYYYMMDDhh (with hour)
use_year_subdirs = False         # Flat directory
file_glob = '*.nc'               # Only .nc files
```

### 3. Date with Dashes
**Filename:** `data_2020-01-01_v2.nc`

```python
date_pattern = r'_(\d{4}-\d{2}-\d{2})_'  # Matches _2020-01-01_
date_format = '%Y-%m-%d'                 # With dashes
use_year_subdirs = False
file_glob = 'data_*.nc'
```

### 4. Timestamp at End
**Filename:** `precipitation.20200101.nc`

```python
date_pattern = r'\.(\d{8})\.'    # Matches .20200101.
date_format = '%Y%m%d'
use_year_subdirs = True
file_glob = 'precipitation.*.nc'
```

### 5. With Minutes/Seconds
**Filename:** `obs_202001011530.nc` (YYYYMMDDhhmm)

```python
date_pattern = r'_(\d{12})\.'    # Matches 202001011530
date_format = '%Y%m%d%H%M'       # With minutes
use_year_subdirs = False
file_glob = 'obs_*.nc'
```

## Regex Pattern Tips

### Basic Patterns
- `\d` - Matches any digit (0-9)
- `\d{8}` - Matches exactly 8 digits
- `\d{4}` - Matches exactly 4 digits
- `\.` - Matches a literal period (escaped)
- `_` - Matches underscore (no escape needed)
- `-` - Matches dash (no escape needed)

### Capturing Groups
Always use parentheses `()` to capture the date string:
```python
r'_(\d{8})_'      # ✓ Correct - captures the date
r'_\d{8}_'        # ✗ Wrong - doesn't capture
```

### Anchoring (Optional)
```python
r'^data_(\d{8})'  # ^ = start of string
r'(\d{8})\.nc$'   # $ = end of string
```

## strptime Format Codes

### Common Codes
- `%Y` - 4-digit year (2020)
- `%y` - 2-digit year (20)
- `%m` - 2-digit month (01-12)
- `%d` - 2-digit day (01-31)
- `%H` - 2-digit hour (00-23)
- `%M` - 2-digit minute (00-59)
- `%S` - 2-digit second (00-59)

### Format Examples
- `'%Y%m%d'` → 20200101
- `'%Y%m%d%H'` → 2020010115
- `'%Y-%m-%d'` → 2020-01-01
- `'%Y%m%d%H%M'` → 202001011530
- `'%Y.%m.%d'` → 2020.01.01

## Testing Your Pattern

To test if your pattern works before running the full pipeline:

```python
import re
from datetime import datetime

# Your filename
filename = "merg_2020123108_10km-pixel.nc"

# Your pattern
date_pattern = r'_(\d{10})_'
date_format = '%Y%m%d%H'

# Test extraction
match = re.search(date_pattern, filename)
if match:
    date_str = match.group(1)
    print(f"Extracted: {date_str}")
    
    # Test parsing
    file_date = datetime.strptime(date_str, date_format)
    print(f"Parsed: {file_date}")
else:
    print("No match found!")
```

## Troubleshooting

### Problem: "No files found"
**Solutions:**
1. Check `base_dir` path is correct
2. Verify `file_glob` pattern matches your files
3. Test your regex pattern separately
4. Check `use_year_subdirs` setting

### Problem: "Could not parse date from filename"
**Solutions:**
1. Ensure `date_pattern` captures the date string with `()`
2. Check `date_format` matches the captured string exactly
3. Test with the code snippet above

### Problem: Files found but date parsing fails
**Solution:** Your `date_format` doesn't match the captured string format.

Example:
```python
# Captured: "2020123108"
date_format = '%Y%m%d'      # ✗ Wrong - only 8 characters
date_format = '%Y%m%d%H'    # ✓ Correct - 10 characters
```

## Directory Structure Options

### Yearly Subdirectories (`use_year_subdirs=True`)
```
base_dir/
  ├── 2020/
  │   ├── file1_20200101.nc
  │   └── file2_20200102.nc
  └── 2021/
      ├── file1_20210101.nc
      └── file2_20210102.nc
```

### Flat Directory (`use_year_subdirs=False`)
```
base_dir/
  ├── file1_20200101.nc
  ├── file2_20200102.nc
  ├── file3_20210101.nc
  └── file4_20210102.nc
```

## Complete Example: ir_imerg

For files like `merg_2020123108_10km-pixel.nc`:

```python
from datetime import datetime
from remap_to_healpix import process_imerg_to_zarr

process_imerg_to_zarr(
    # Date range (include hours for hourly data)
    start_date=datetime(2020, 12, 31, 8),
    end_date=datetime(2020, 12, 31, 18),
    
    # Output configuration
    zoom=9,
    output_zarr="/path/to/output.zarr",
    input_base_dir="/data/ir_imerg",
    
    # File pattern configuration
    date_pattern=r'_(\d{10})_',     # Extract YYYYMMDDhh
    date_format='%Y%m%d%H',          # Parse with hour
    use_year_subdirs=False,          # Flat directory
    file_glob='merg_*.nc',           # Match merg files
    
    # Processing options
    time_average=None,
    convert_time=True,
    overwrite=True
)
```
