# Dataset-Specific Preprocessing Guide

This guide explains how to use the flexible preprocessing system in `process_to_healpix_zarr()`.

## Overview

Instead of adding a new flag for each dataset-specific operation, you can now pass **callable functions** directly to the pipeline. This provides maximum flexibility and keeps the main function signature clean.

## Basic Usage

### Single Preprocessing Function

```python
from datetime import datetime
from remap_to_healpix import process_to_healpix_zarr
from src.preprocessing import subset_time_by_minute

# IR_IMERG with time subsetting
config = {
    'input_base_dir': '/data/ir_imerg',
    'time_chunk_size': 24,
    'convert_time': True,
    'date_pattern': r'_(\d{10})_',
    'date_format': '%Y%m%d%H'
}

process_to_healpix_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    zoom=9,
    output_zarr="/path/to/output.zarr",
    weights_file="/path/to/weights/ir_imerg_z9_weights.nc",
    preprocessing_func=subset_time_by_minute,
    preprocessing_kwargs={'time_subset': '00min'},
    config=config
)
```

### Multiple Preprocessing Functions (Chained)

```python
from src.preprocessing import subset_time_by_minute, apply_quality_mask, subset_by_region

# Apply multiple preprocessing steps in sequence
config = {
    'input_base_dir': '/data/satellite',
    'time_chunk_size': 24,
    'convert_time': True
}

process_to_healpix_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    zoom=9,
    output_zarr="/path/to/output.zarr",
    weights_file="/path/to/weights/satellite_z9_weights.nc",
    preprocessing_func=[
        subset_by_region,
        subset_time_by_minute,
        apply_quality_mask
    ],
    preprocessing_kwargs=[
        {'lat_bounds': (-30, 30), 'lon_bounds': None},  # Tropics only
        {'time_subset': '00min'},
        {'quality_threshold': 0.8}
    ],
    config=config
)
```

## Available Preprocessing Functions

### 1. `subset_time_by_minute(ds, time_subset)`

**Purpose**: Select specific minute values within each hour (IMERG/IR_IMERG specific)

**Parameters**:
- `time_subset`: `'00min'` or `'30min'`

**Use Case**: Reduce output by ~50% when hourly-averaged data is stored at 30-minute intervals

**Example**:
```python
preprocessing_func=subset_time_by_minute,
preprocessing_kwargs={'time_subset': '00min'}
```

### 2. `apply_quality_mask(ds, quality_var, quality_threshold, mask_value)`

**Purpose**: Mask data based on quality flags

**Parameters**:
- `quality_var`: Name of quality variable (default: `'quality_flag'`)
- `quality_threshold`: Minimum quality (0-1 scale, default: `0.8`)
- `mask_value`: Value for masked data (default: `None` for NaN)

**Example**:
```python
preprocessing_func=apply_quality_mask,
preprocessing_kwargs={'quality_var': 'QC', 'quality_threshold': 0.9}
```

### 3. `subset_by_region(ds, lat_bounds, lon_bounds)`

**Purpose**: Subset to a geographic region before remapping

**Parameters**:
- `lat_bounds`: Tuple of `(min, max)` latitude
- `lon_bounds`: Tuple of `(min, max)` longitude

**Example**:
```python
preprocessing_func=subset_by_region,
preprocessing_kwargs={'lat_bounds': (20, 50), 'lon_bounds': (-120, -70)}
```

## Creating Custom Preprocessing Functions

You can easily create your own preprocessing functions! Just follow this template:

```python
# In src/preprocessing.py

def my_custom_preprocessing(ds, param1, param2=default_value):
    """
    Description of what this preprocessing does.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset
    param1 : type
        Description
    param2 : type, optional
        Description (default: default_value)
    
    Returns:
    --------
    xr.Dataset : Processed dataset
    """
    logger.info(f"Applying custom preprocessing with param1={param1}")
    
    # Your processing logic here
    ds_processed = ds.copy()
    # ... do something with ds_processed ...
    
    return ds_processed
```

Then use it:

```python
from src.preprocessing import my_custom_preprocessing

config = {
    'input_base_dir': '/data/input',
    # ... other config options
}

process_to_healpix_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    zoom=9,
    output_zarr="/path/to/output.zarr",
    weights_file="/path/to/weights.nc",
    preprocessing_func=my_custom_preprocessing,
    preprocessing_kwargs={'param1': 'value', 'param2': 123},
    config=config
)
```

## Advanced: Lambda Functions

For simple one-off operations, you can use lambda functions:

```python
# Remove a specific variable
process_to_healpix_zarr(
    ...,
    preprocessing_func=lambda ds: ds.drop_vars('unwanted_var')
)

# Scale a variable
process_to_healpix_zarr(
    ...,
    preprocessing_func=lambda ds, scale_factor: ds.assign(temp=ds.temp * scale_factor),
    preprocessing_kwargs={'scale_factor': 0.1}
)
```

## Backward Compatibility

The old `time_subset` parameter is still supported but deprecated:

```python
# Old way (still works, but shows deprecation warning)
process_to_healpix_zarr(
    ...,
    time_subset='00min'  # ⚠️ Deprecated
)

# New way (recommended)
process_to_healpix_zarr(
    ...,
    preprocessing_func=subset_time_by_minute,
    preprocessing_kwargs={'time_subset': '00min'}
)
```

## Benefits of This Approach

1. **Extensible**: Add new preprocessing functions without modifying the main function
2. **Composable**: Chain multiple preprocessing steps easily
3. **Reusable**: Preprocessing functions can be used independently
4. **Type-safe**: IDEs can provide autocomplete and type checking
5. **Testable**: Each preprocessing function can be tested in isolation
6. **Clean**: Main function signature doesn't grow with each new dataset type

## Example Workflows

### Workflow 1: IMERG Standard Processing
```python
# No preprocessing needed
config = {
    'input_base_dir': '/data/IMERG',
    'time_average': '1h'
}

process_to_healpix_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    zoom=9,
    output_zarr="imerg_output.zarr",
    weights_file="/path/to/weights/imerg_z9_weights.nc",
    config=config
)
```

### Workflow 2: IR_IMERG with Time Subsetting
```python
from src.preprocessing import subset_time_by_minute

config = {
    'input_base_dir': '/data/ir_imerg',
    'date_pattern': r'_(\d{10})_',
    'date_format': '%Y%m%d%H'
}

process_to_healpix_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    zoom=9,
    output_zarr="ir_imerg_output.zarr",
    weights_file="/path/to/weights/ir_imerg_z9_weights.nc",
    preprocessing_func=subset_time_by_minute,
    preprocessing_kwargs={'time_subset': '00min'},
    config=config
)
```

### Workflow 3: Regional High-Quality Subset
```python
from src.preprocessing import subset_by_region, apply_quality_mask

config = {
    'input_base_dir': '/data/satellite'
}

process_to_healpix_zarr(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    zoom=9,
    output_zarr="regional_quality_output.zarr",
    weights_file="/path/to/weights/satellite_z9_weights.nc",
    preprocessing_func=[subset_by_region, apply_quality_mask],
    preprocessing_kwargs=[
        {'lat_bounds': (-10, 10), 'lon_bounds': (100, 150)},  # Maritime Continent
        {'quality_threshold': 0.9, 'quality_var': 'QC_flag'}
    ],
    config=config
)
```

