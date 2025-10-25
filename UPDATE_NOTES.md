# Update Summary: File Pattern Configuration Added

## Changes Made

### 1. Updated Configuration File: `config/tb_imerg_config.yaml`

Added new file search pattern parameters:

```yaml
# File search pattern configuration
date_pattern: "_(\\d{10})_"    # Regex to extract YYYYMMDDhh
date_format: "%Y%m%d%H"        # strptime format for parsing
use_year_subdirs: true         # Files in yearly subdirectories
file_glob: "merg_*.nc"         # File matching pattern
```

**Note on YAML escaping**: In YAML, use double quotes and double backslash `"_(\\d{10})_"` for regex patterns.

### 2. Updated Launch Script: `launch_ir_imerg_processing.py`

Added code to read and pass file pattern parameters:

```python
# Get file search pattern configuration from config file
date_pattern = config.get('date_pattern', r'\.(\d{8})-')
date_format = config.get('date_format', '%Y%m%d')
use_year_subdirs = config.get('use_year_subdirs', True)
file_glob = config.get('file_glob', '*.nc*')

# Pass to process_imerg_to_zarr
process_imerg_to_zarr(
    ...,
    date_pattern=date_pattern,
    date_format=date_format,
    use_year_subdirs=use_year_subdirs,
    file_glob=file_glob,
)
```

The script now:
- Reads pattern configuration from YAML
- Displays pattern settings when running
- Passes patterns to the processing function
- Has fallback defaults for backwards compatibility

### 3. Created Test Script: `test_ir_imerg_pattern.py`

Quick test utility specific to your IR_IMERG setup:
- Tests pattern against sample filename
- Scans actual directory and shows results
- Verifies configuration before running full processing

### 4. Created Quick Reference: `IR_IMERG_QUICKSTART.md`

Comprehensive guide covering:
- Your specific file pattern configuration
- Usage examples
- Troubleshooting
- Configuration options

## Your IR_IMERG Configuration

### Filename Format
```
merg_2020010100_10km-pixel.nc
     YYYYMMDDhh
```

### Pattern Configuration
- **Pattern**: `"_(\\d{10})_"` - Matches 10 digits between underscores
- **Format**: `"%Y%m%d%H"` - Parses as year-month-day-hour
- **Structure**: Yearly subdirectories (`2020/`, `2021/`, etc.)
- **Glob**: `"merg_*.nc"` - Files starting with "merg_"

### Directory Structure
```
/pscratch/sd/w/wcmca1/GPM/IR_IMERG_Combined_V07B/
├── 2020/
│   ├── merg_2020010100_10km-pixel.nc  (Jan 1, 2020, 00:00)
│   ├── merg_2020010101_10km-pixel.nc  (Jan 1, 2020, 01:00)
│   └── ... (8784 files)
└── 2021/
    └── ...
```

## Testing

Verify the configuration works:

```bash
cd /global/homes/f/feng045/program/hackathon/remap_imerg
python test_ir_imerg_pattern.py
```

Expected output:
```
✅ SUCCESS: Pattern configuration is correct!
✅ All test files processed successfully!
```

## Usage

Run your processing with the same command as before:

```bash
python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9
```

The script will now:
1. Load file pattern config from `tb_imerg_config.yaml`
2. Display pattern settings
3. Find files matching your pattern in the date range
4. Process them to HEALPix format

## Backwards Compatibility

All existing functionality is preserved:
- If pattern parameters are missing from config, defaults are used
- Original IMERG workflows still work unchanged
- You can still override parameters programmatically if needed

## Files Modified

1. ✅ `config/tb_imerg_config.yaml` - Added pattern parameters
2. ✅ `launch_ir_imerg_processing.py` - Updated to use patterns
3. ✅ `remap_to_healpix.py` - Already updated (previous changes)

## Files Created

1. 📄 `test_ir_imerg_pattern.py` - Testing utility
2. 📄 `IR_IMERG_QUICKSTART.md` - Quick reference guide

## Next Steps

1. **Test the configuration**:
   ```bash
   python test_ir_imerg_pattern.py
   ```

2. **Run a small test** (single day):
   ```bash
   python launch_ir_imerg_processing.py 2020-01-01 2020-01-01 9 --overwrite
   ```

3. **Run full processing** (if test successful):
   ```bash
   python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9
   ```

## Troubleshooting

### Pattern doesn't match
- Check YAML escaping: use `"_(\\d{10})_"` with double quotes and double backslash
- Test with `test_ir_imerg_pattern.py`

### Wrong files selected
- Verify `date_format` includes all components (year, month, day, hour)
- Check date range includes hour if needed: `datetime(2020, 1, 1, 0)`

### No files found
- Verify `use_year_subdirs: true` matches your directory structure
- Check `file_glob: "merg_*.nc"` matches your filenames

## Configuration Tips

### Change time averaging
Edit `config/tb_imerg_config.yaml`:
```yaml
time_average: "3h"  # Options: null, "1h", "3h", "6h", "1d"
```

### Change zoom level default
```yaml
default_zoom: 11  # Higher number = higher resolution
```

### Adjust memory usage
```yaml
time_chunk_size: 12  # Reduce if running out of memory
```

## Summary

Your IR_IMERG data processing is now fully configured with:
- ✅ Automatic file discovery based on date patterns
- ✅ Flexible pattern matching for different filename formats
- ✅ Configuration-driven approach (no code changes needed)
- ✅ Testing utilities to verify setup
- ✅ Backwards compatible with existing workflows

The same generalized framework can be used for other datasets by simply changing the pattern configuration!
