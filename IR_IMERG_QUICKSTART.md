# IR_IMERG Processing Quick Reference

## File Pattern Configuration

Your IR_IMERG data has been configured with the following file search pattern:

### Filename Format
```
merg_2020010100_10km-pixel.nc
      └─────┬────┘
     YYYYMMDDhh (10 digits)
```

### Configuration (in `config/tb_imerg_config.yaml`)
```yaml
date_pattern: "_(\\d{10})_"    # Extracts YYYYMMDDhh between underscores
date_format: "%Y%m%d%H"        # Parses year, month, day, hour
use_year_subdirs: true         # Files in yearly subdirs (2020/, 2021/, etc.)
file_glob: "merg_*.nc"         # Matches files starting with "merg_"
```

### How It Works

1. **File Discovery**: Scans `/pscratch/.../GPM/IR_IMERG_Combined_V07B/{year}/` for files matching `merg_*.nc`

2. **Date Extraction**: Uses regex `_(\\d{10})_` to extract date string
   - Example: `merg_2020010100_10km-pixel.nc` → extracts `"2020010100"`

3. **Date Parsing**: Uses format `%Y%m%d%H` to parse the string
   - `2020010100` → `datetime(2020, 1, 1, 0)` (Jan 1, 2020, 00:00)

4. **Date Filtering**: Only includes files within your specified date range

## Usage

### Basic Usage
```bash
# Process entire year with default settings
python launch_ir_imerg_processing.py 2020-01-01 2020-12-31

# Process with specific zoom level
python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9

# Overwrite existing output
python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9 --overwrite
```

### Testing Before Running

Always test the configuration first:
```bash
python test_ir_imerg_pattern.py
```

This will:
- Verify the regex pattern matches your filenames
- Test date extraction and parsing
- Show sample files that will be processed

## Configuration File

Edit `config/tb_imerg_config.yaml` to customize:

### Paths
```yaml
input_base_dir: "/pscratch/sd/w/wcmca1/GPM/IR_IMERG_Combined_V07B/"
output_base_dir: "/pscratch/sd/w/wcmca1/GPM/healpix/"
weights_dir: "/pscratch/sd/w/wcmca1/GPM/weights/"
```

### Processing
```yaml
default_zoom: 9           # HEALPix resolution
time_chunk_size: 24       # Chunks for processing (24 = 1 day)
time_average: null        # Temporal averaging (null, "1h", "3h", "6h", "1d")
convert_time: true        # Convert to standard datetime64
```

### File Pattern (Already Configured)
```yaml
date_pattern: "_(\\d{10})_"
date_format: "%Y%m%d%H"
use_year_subdirs: true
file_glob: "merg_*.nc"
```

## Examples

### Process single month
```bash
python launch_ir_imerg_processing.py 2020-01-01 2020-01-31 9
```

### Process with hourly averaging
Edit `config/tb_imerg_config.yaml`:
```yaml
time_average: "1h"
```
Then run:
```bash
python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9
```

### Process with 3-hourly averaging
Edit config:
```yaml
time_average: "3h"
```
Then run:
```bash
python launch_ir_imerg_processing.py 2020-01-01 2020-12-31 9
```

## Output

Output files are named based on your settings:
```
IR_IMERG_V7_zoom9_20200101_20201231.zarr              # No averaging
IR_IMERG_V7_1H_zoom9_20200101_20201231.zarr           # 1-hour average
IR_IMERG_V7_3H_zoom9_20200101_20201231.zarr           # 3-hour average
```

## Troubleshooting

### No files found
```bash
# Check if pattern is correct
python test_ir_imerg_pattern.py

# Check directory structure
ls /pscratch/sd/w/wcmca1/GPM/IR_IMERG_Combined_V07B/2020/ | head
```

### Date parsing errors
If filenames change, update in `config/tb_imerg_config.yaml`:
- `date_pattern`: Regex to extract date
- `date_format`: Format to parse the date

### Memory issues
Reduce in `config/tb_imerg_config.yaml`:
```yaml
time_chunk_size: 12  # Instead of 24
```

## Directory Structure

Your data is organized as:
```
/pscratch/sd/w/wcmca1/GPM/IR_IMERG_Combined_V07B/
├── 1998/
│   └── merg_1998010100_10km-pixel.nc
│   └── merg_1998010101_10km-pixel.nc
│   └── ...
├── 2020/
│   └── merg_2020010100_10km-pixel.nc
│   └── merg_2020010101_10km-pixel.nc  (8784 files for 2020)
│   └── ...
└── 2024/
    └── ...
```

This matches the configuration:
- `use_year_subdirs: true` ✓
- `file_glob: "merg_*.nc"` ✓

## Verification

Run this to verify configuration:
```bash
cd /global/homes/f/feng045/program/hackathon/remap_imerg
python test_ir_imerg_pattern.py
```

Expected output:
```
✅ SUCCESS: Pattern configuration is correct!
✅ All test files processed successfully!
```

## Additional Resources

- `FILE_PATTERN_GUIDE.md` - Comprehensive pattern configuration guide
- `VISUAL_GUIDE.md` - Visual explanation of how patterns work
- `example_usage.py` - Examples for different data formats
- `test_file_pattern.py` - General pattern testing tool
