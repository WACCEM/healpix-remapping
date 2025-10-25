# Generalization of remap_imerg_to_zarr Workflow

## Summary

The `remap_to_healpix.py` module has been generalized to work with any gridded lat/lon NetCDF dataset, not just IMERG data. The key improvement is flexible file pattern matching that allows users to specify how dates are formatted in filenames.

## Changes Made

### 1. Enhanced `get_imerg_files()` Function

**New Parameters:**
- `date_pattern` (str): Regex pattern to extract date string from filename
  - Default: `r'\.(\d{8})-'` (IMERG format)
  - Example for ir_imerg: `r'_(\d{10})_'`
  
- `date_format` (str): strptime format string to parse the captured date
  - Default: `'%Y%m%d'` (YYYYMMDD)
  - Example for ir_imerg: `'%Y%m%d%H'` (YYYYMMDDhh)
  
- `use_year_subdirs` (bool): Whether to search in yearly subdirectories
  - Default: `True` (original IMERG structure)
  - Set to `False` for flat directory structures
  
- `file_glob` (str): Glob pattern for file matching
  - Default: `'*.nc*'`
  - Can be more specific: `'merg_*.nc'`

**New Functionality:**
- Flexible regex-based date extraction
- Support for both hierarchical (year-based) and flat directory structures
- Better error handling and validation
- Detailed logging for debugging

### 2. New Helper Function: `filter_files_by_date()`

Separates date filtering logic for better code organization and reusability.

### 3. Updated `process_imerg_to_zarr()` Function

Now accepts all the new file pattern parameters and passes them through to `get_imerg_files()`.

### 4. Updated Documentation

- Module docstring with examples for different formats
- Enhanced function docstrings with parameter details
- Added usage examples

## New Files Created

### 1. `example_usage.py`
Demonstrates how to use the generalized workflow with:
- Original IMERG format
- ir_imerg format (hourly data with YYYYMMDDhh)
- Generic formats with different naming conventions

### 2. `FILE_PATTERN_GUIDE.md`
Comprehensive guide covering:
- Quick reference table for parameters
- Common filename pattern examples
- Regex pattern tips and tricks
- strptime format codes
- Troubleshooting guide
- Directory structure options

### 3. `test_file_pattern.py`
Testing utility to verify patterns before running the full pipeline:
- Test individual filenames
- Scan directories and test extraction
- Validate date filtering
- Built-in test cases for common formats

## Usage for ir_imerg Data

For files like `merg_2020123108_10km-pixel.nc`:

```python
from datetime import datetime
from remap_to_healpix import process_imerg_to_zarr

process_imerg_to_zarr(
    # Date range with hours
    start_date=datetime(2020, 12, 31, 8),
    end_date=datetime(2020, 12, 31, 18),
    
    # Output
    zoom=9,
    output_zarr="/path/to/output.zarr",
    input_base_dir="/data/ir_imerg",
    
    # ir_imerg-specific configuration
    date_pattern=r'_(\d{10})_',      # Match _YYYYMMDDhh_
    date_format='%Y%m%d%H',          # Parse with hour
    use_year_subdirs=False,          # Flat directory
    file_glob='merg_*.nc',           # Match merg files only
    
    # Processing
    time_average=None,
    convert_time=True,
    overwrite=True
)
```

## Backwards Compatibility

The changes are **fully backwards compatible**. The default parameter values maintain the original IMERG behavior, so existing code will work without modification:

```python
# This still works exactly as before
process_imerg_to_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 1, 31),
    zoom=9,
    output_zarr="/path/to/output.zarr",
    input_base_dir="/path/to/IMERG",
    time_average="1h"
)
```

## Testing Workflow

Before running the full pipeline on a new dataset:

1. **Test the pattern on a sample filename:**
   ```bash
   python test_file_pattern.py
   ```

2. **Verify directory scanning:**
   - Edit `test_file_pattern.py` 
   - Uncomment and configure the `test_directory_scan()` call
   - Run to see which files will be found

3. **Run the full pipeline:**
   ```python
   python example_usage.py  # or your custom script
   ```

## Key Benefits

1. **Flexibility**: Works with any filename convention
2. **Maintainability**: Single codebase for all datasets
3. **Robustness**: Better error handling and validation
4. **Documentation**: Comprehensive guides and examples
5. **Testing**: Tools to verify configuration before processing
6. **Backwards Compatible**: Existing IMERG workflows unchanged

## Next Steps

To adapt for your specific dataset:

1. Identify the date pattern in your filenames
2. Construct the appropriate regex pattern (use `test_file_pattern.py`)
3. Specify the matching strptime format
4. Configure directory structure settings
5. Test with a small date range first
6. Scale up to full processing

## Questions?

See `FILE_PATTERN_GUIDE.md` for detailed examples and troubleshooting tips.
