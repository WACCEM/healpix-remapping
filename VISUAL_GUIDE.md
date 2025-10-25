# Visual Guide: File Pattern Matching

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                     Your Data Files                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  merg_2020123108_10km-pixel.nc                           │   │
│  │  merg_2020123109_10km-pixel.nc                           │   │
│  │  merg_2020123110_10km-pixel.nc                           │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Step 1: File Discovery (glob)                      │
│                                                                 │
│  file_glob = 'merg_*.nc'                                        │
│  use_year_subdirs = False                                       │
│                                                                 │
│  Finds: All files matching pattern in base_dir/                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│         Step 2: Extract Date String (regex)                     │
│                                                                 │
│  Filename:    merg_2020123108_10km-pixel.nc                     │
│               ─────┬──────────                                  │
│  Pattern:     r'_(\d{10})_'    (capture 10 digits)              │
│               ─────┬──────────                                  │
│  Extracted:   "2020123108"     ← captured group                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│         Step 3: Parse Date String (strptime)                    │
│                                                                 │
│  String:      "2020123108"                                      │
│  Format:      '%Y%m%d%H'                                        │
│               ─────┬──────                                      │
│  Mapping:     Year │Month│Day│Hour                              │
│               2020 │12  │31│08                                  │
│               ─────┴────┴──┴──                                  │
│  Result:      datetime(2020, 12, 31, 8)                         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│            Step 4: Filter by Date Range                         │
│                                                                 │
│  start_date = datetime(2020, 12, 31, 8)   ✓ Include             │
│  file_date  = datetime(2020, 12, 31, 8)   ← File date           │
│  end_date   = datetime(2020, 12, 31, 18)  ✓ Include             │
│                                                                 │
│  Keep file: YES (within range)                                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   Filtered File List                            │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  merg_2020123108_10km-pixel.nc  ✓                        │   │
│  │  merg_2020123109_10km-pixel.nc  ✓                        │   │
│  │  merg_2020123110_10km-pixel.nc  ✓                        │   │
│  │  ...                                                     │   │
│  │  merg_2020123118_10km-pixel.nc  ✓                        │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Pattern Components Explained

### Regex Pattern: `r'_(\d{10})_'`

```
  _  ( \d{10} )  _
  │  │    │    │ │
  │  │    │    │ └─ Literal underscore (must match)
  │  │    │    └─── End capture group
  │  │    └──────── Match exactly 10 digits (\d = any digit 0-9)
  │  └───────────── Start capture group (extracts matched text)
  └──────────────── Literal underscore (must match)

Full filename:  merg_2020123108_10km-pixel.nc
                     └──────────┘
                   Matched & captured
```

### strptime Format: `'%Y%m%d%H'`

```
  Captured string: "2020123108"
  Format string:   "%Y%m%d%H"
                    ││ │ │ └─ %H = Hour (00-23)
                    ││ │ └─── %d = Day (01-31)
                    ││ └───── %m = Month (01-12)
                    │└─────── %Y = Year (4 digits)
                    └──────── Each % code consumes characters
  
  Parsing:
  "2020123108"
   ││││││││└└─ → hour = 08
   ││││││└└─── → day = 31
   ││││└└───── → month = 12
   └└└└─────── → year = 2020
  
  Result: datetime(2020, 12, 31, 8, 0, 0)
```

## Common Patterns Comparison

### 1. IMERG Format
```
Filename:  3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4
           ─────────────────────────┬────────────────────────────────────────
Pattern:                       r'\.(\d{8})-'
                                   └────┘ captures "20200101"
Format:                        '%Y%m%d'
Result:                        datetime(2020, 1, 1)
```

### 2. ir_imerg Format
```
Filename:  merg_2020123108_10km-pixel.nc
           ─────┬──────────
Pattern:   r'_(\d{10})_'
               └──────┘ captures "2020123108"
Format:    '%Y%m%d%H'
Result:    datetime(2020, 12, 31, 8)
```

### 3. Generic with Dashes
```
Filename:  data_2020-01-01_v2.nc
           ─────┬──────────
Pattern:   r'_(\d{4}-\d{2}-\d{2})_'
               └────────┘ captures "2020-01-01"
Format:    '%Y-%m-%d'
Result:    datetime(2020, 1, 1)
```

## Testing Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Test Your Pattern                            │
│                                                                 │
│  1. Create sample filename:                                     │
│     filename = "merg_2020123108_10km-pixel.nc"                  │
│                                                                 │
│  2. Define pattern:                                             │
│     pattern = r'_(\d{10})_'                                     │
│     format = '%Y%m%d%H'                                         │
│                                                                 │
│  3. Test with test_file_pattern.py:                             │
│     $ python test_file_pattern.py                               │
│                                                                 │
│     ✓ Pattern matched!                                          │
│       Extracted string: '2020123108'                            │
│     ✓ Date parsed successfully!                                 │
│       Parsed datetime: 2020-12-31 08:00:00                      │
│     ✅ SUCCESS: Pattern works correctly!                        │
└─────────────────────────────────────────────────────────────────┘
```

## Decision Tree: Which Pattern Type?

```
Your filename has...
│
├─ Date with separators (2020-01-01, 2020.01.01)?
│  │
│  ├─ With dashes → Pattern: r'(\d{4}-\d{2}-\d{2})'
│  │                 Format: '%Y-%m-%d'
│  │
│  └─ With dots   → Pattern: r'(\d{4}\.\d{2}\.\d{2})'
│                    Format: '%Y.%m.%d'
│
└─ Date without separators (20200101, 2020123108)?
   │
   ├─ YYYYMMDD (8 digits)     → Pattern: r'(\d{8})'
   │                             Format: '%Y%m%d'
   │
   ├─ YYYYMMDDhh (10 digits)  → Pattern: r'(\d{10})'
   │                             Format: '%Y%m%d%H'
   │
   └─ YYYYMMDDhhmm (12 digits)→ Pattern: r'(\d{12})'
                                 Format: '%Y%m%d%H%M'
```

## Directory Structure Options

### Option 1: Yearly Subdirectories (use_year_subdirs=True)

```
/data/
├── 2020/
│   ├── file_20200101.nc
│   ├── file_20200102.nc
│   └── ...
└── 2021/
    ├── file_20210101.nc
    ├── file_20210102.nc
    └── ...

Config:  use_year_subdirs = True
Behavior: Scans /data/{year}/ for each year in date range
```

### Option 2: Flat Directory (use_year_subdirs=False)

```
/data/
├── file_20200101.nc
├── file_20200102.nc
├── file_20210101.nc
├── file_20210102.nc
└── ...

Config:  use_year_subdirs = False
Behavior: Scans /data/ directly
```

## Complete Example Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  Configuration                                                  │
├─────────────────────────────────────────────────────────────────┤
│  input_base_dir:  "/data/ir_imerg"                              │
│  date_pattern:    r'_(\d{10})_'                                 │
│  date_format:     '%Y%m%d%H'                                    │
│  file_glob:       'merg_*.nc'                                   │
│  use_year_subdirs: False                                        │
│  start_date:      datetime(2020, 12, 31, 8)                     │
│  end_date:        datetime(2020, 12, 31, 18)                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  File Discovery                                                 │
├─────────────────────────────────────────────────────────────────┤
│  Glob: /data/ir_imerg/merg_*.nc                                 │
│  Found: 100 files                                               │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  Date Extraction & Filtering                                    │
├─────────────────────────────────────────────────────────────────┤
│  Processing each file...                                        │
│    merg_2020123108_10km-pixel.nc → 2020-12-31 08:00 ✓ Include   │
│    merg_2020123109_10km-pixel.nc → 2020-12-31 09:00 ✓ Include   │
│    ...                                                          │
│    merg_2020123118_10km-pixel.nc → 2020-12-31 18:00 ✓ Include   │
│    merg_2020123119_10km-pixel.nc → 2020-12-31 19:00 ✗ Exclude   │
│                                                                 │
│  Result: 11 files in date range                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  Processing Pipeline                                            │
├─────────────────────────────────────────────────────────────────┤
│  1. Read files → xarray Dataset                                 │
│  2. Temporal average (if configured)                            │
│  3. Remap to HEALPix                                            │
│  4. Write to Zarr                                               │
└─────────────────────────────────────────────────────────────────┘
```
