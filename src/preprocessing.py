#!/usr/bin/env python3
"""
Dataset-specific preprocessing functions.

This module contains preprocessing functions that are specific to certain datasets
(e.g., IMERG, IR_IMERG, ERA5) and are not part of the generic remapping pipeline.
These functions can be applied as optional preprocessing steps before remapping.
"""

import pandas as pd
import logging
from src.utilities import _cftime_to_timestamp

logger = logging.getLogger(__name__)


def subset_time_by_minute(ds, time_subset):
    """
    Subset dataset by selecting specific minute values within each hour.
    
    This function is useful when data at different time resolutions have identical 
    values (e.g., hourly-averaged precipitation stored at 30-minute intervals).
    
    Common use case: IMERG and IR_IMERG datasets where hourly-averaged data is
    stored at both 00 and 30 minutes, reducing output size by ~50% by selecting
    only one timestamp per hour.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with time dimension
    time_subset : str
        Minute selection option:
        - '00min': Keep only times at ~00 minutes (e.g., 00:00, 01:00, 02:00)
        - '30min': Keep only times at ~30 minutes (e.g., 00:30, 01:30, 02:30)
    
    Returns:
    --------
    xr.Dataset : Dataset with subset of time steps
    
    Raises:
    -------
    ValueError : If time_subset is not '00min' or '30min'
    
    Notes:
    ------
    Uses ±5 minute tolerance to handle slight timing variations in data files.
    Handles both cftime and numpy datetime64 time coordinates.
    
    Examples:
    ---------
    >>> import xarray as xr
    >>> from src.preprocessing import subset_time_by_minute
    >>> 
    >>> # Load IMERG data with 30-minute intervals
    >>> ds = xr.open_dataset('imerg_data.nc')
    >>> 
    >>> # Keep only hourly timestamps at 00 minutes
    >>> ds_hourly = subset_time_by_minute(ds, '00min')
    """
    logger.info(f"Subsetting time steps by minute value: '{time_subset}'")
    original_times = ds.sizes['time']
    
    # Extract minute values from time coordinate
    # Handle both cftime and numpy datetime64
    if hasattr(ds.time.values[0], 'minute'):
        # cftime objects
        minutes = [t.minute for t in ds.time.values]
    else:
        # numpy datetime64 - convert to pandas for minute extraction
        minutes = pd.to_datetime(ds.time.values).minute.values
    
    # Create mask based on time_subset parameter
    if time_subset == '00min':
        # Keep times close to 00 minutes (within ±5 minutes tolerance)
        mask = [abs(m) <= 5 or abs(m - 60) <= 5 for m in minutes]
        logger.info(f"Keeping time steps at approximately 00 minutes per hour")
    elif time_subset == '30min':
        # Keep times close to 30 minutes (within ±5 minutes tolerance)
        mask = [abs(m - 30) <= 5 for m in minutes]
        logger.info(f"Keeping time steps at approximately 30 minutes per hour")
    else:
        raise ValueError(f"Invalid time_subset value: '{time_subset}'. Must be '00min' or '30min'")
    
    # Apply the mask
    ds_subset = ds.isel({'time': mask})
    subset_times = ds_subset.sizes['time']
    
    logger.info(f"Time steps: {original_times} → {subset_times} ({subset_times/original_times*100:.1f}% retained)")
    logger.info(f"Reduction: {original_times - subset_times} time steps removed")
    
    return ds_subset


def subset_vertical_levels(ds, level_subset, z_coordname='level'):
    """
    Subset dataset by selecting specific vertical levels.
    
    This function selects specific vertical levels (e.g., pressure levels) from
    3D atmospheric data while maintaining lazy evaluation with Dask. Useful for
    reducing data size by keeping only commonly-used levels.
    
    Common use case: ERA5 3D data has 37 pressure levels, but analysis often only
    needs ~15 levels. Subsetting reduces file size and processing time by ~60%.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with vertical coordinate
    level_subset : list of numeric
        List of level values to keep (e.g., [1000, 925, 850, 700, 500, 300, 200, 100])
        Values must match coordinate values in dataset
    z_coordname : str, optional
        Name of vertical coordinate dimension (default: 'level')
        Common names: 'level', 'plev', 'pressure', 'lev', 'z'
    
    Returns:
    --------
    xr.Dataset : Dataset with subset of vertical levels (lazy)
    
    Raises:
    -------
    ValueError : If z_coordname not found in dataset
    ValueError : If no valid levels found in dataset
    
    Notes:
    ------
    - Maintains lazy evaluation - does NOT load data into memory
    - Uses index-based selection compatible with Dask arrays
    - Preserves all data variables and attributes
    - Warns if requested levels not found in dataset
    - Order of output levels matches order in level_subset
    
    Examples:
    ---------
    >>> import xarray as xr
    >>> from src.preprocessing import subset_vertical_levels
    >>> 
    >>> # Load ERA5 3D data with 37 pressure levels
    >>> ds = xr.open_dataset('era5_3d.nc')
    >>> 
    >>> # Keep only tropospheric levels
    >>> levels = [1000, 925, 850, 700, 500, 400, 300, 250, 200, 150, 100]
    >>> ds_trop = subset_vertical_levels(ds, levels)
    >>> 
    >>> # Keep only standard mandatory levels
    >>> levels = [1000, 850, 700, 500, 400, 300, 250, 200, 150, 100]
    >>> ds_std = subset_vertical_levels(ds, levels)
    >>> 
    >>> # Use custom vertical coordinate name
    >>> ds_subset = subset_vertical_levels(ds, levels, z_coordname='plev')
    """
    logger.info(f"Subsetting vertical levels using coordinate: '{z_coordname}'")
    
    # Check if vertical coordinate exists
    if z_coordname not in ds.coords:
        raise ValueError(
            f"Vertical coordinate '{z_coordname}' not found in dataset. "
            f"Available coordinates: {list(ds.coords.keys())}"
        )
    
    original_levels = ds.sizes[z_coordname]
    
    # Get all level values from dataset
    all_levels = ds[z_coordname].values
    
    # Find indices of requested levels
    level_indices = []
    missing_levels = []
    
    for requested_level in level_subset:
        # Find index where level matches (within small tolerance for floating point)
        matches = [i for i, lev in enumerate(all_levels) if abs(lev - requested_level) < 0.01]
        
        if matches:
            level_indices.append(matches[0])
        else:
            missing_levels.append(requested_level)
    
    if missing_levels:
        logger.warning(
            f"Requested levels not found in dataset: {missing_levels}\n"
            f"Available levels: {sorted(all_levels)}"
        )
    
    if not level_indices:
        raise ValueError(
            f"No valid levels found in dataset! "
            f"Requested: {level_subset}\n"
            f"Available: {sorted(all_levels)}"
        )
    
    logger.info(f"Selecting {len(level_indices)} levels out of {original_levels} total")
    logger.info(f"Selected levels: {[all_levels[i] for i in level_indices]}")
    
    # Use index-based selection to maintain lazy evaluation
    # This is compatible with Dask and doesn't load data into memory
    ds_subset = ds.isel({z_coordname: level_indices})
    
    subset_levels = ds_subset.sizes[z_coordname]
    
    logger.info(f"Vertical levels: {original_levels} → {subset_levels} ({subset_levels/original_levels*100:.1f}% retained)")
    logger.info(f"Reduction: {original_levels - subset_levels} levels removed")
    
    # Add attribute to track the subsetting
    ds_subset.attrs['level_subset_note'] = f'Vertical dimension subset to {len(level_subset)} specified levels using index selection'
    ds_subset.attrs['level_subset_values'] = str(level_subset)
    
    return ds_subset


def subset_time_by_interval(ds, time_subset, time_dim='time'):
    """
    Subset dataset by selecting time steps at regular intervals.
    
    This function selects time steps at specified intervals while maintaining
    lazy evaluation with Dask. Useful for reducing temporal resolution before 
    remapping to save computation time and storage space.
    
    Common use case: ERA5 hourly data reduced to 3-hourly or 6-hourly intervals
    for climate analysis where sub-daily resolution is not needed.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with time dimension
    time_subset : str
        Time interval for subsetting (pandas frequency string):
        - '1h' or '1H': Hourly (every 1 time step if already hourly)
        - '3h' or '3H': Every 3 hours
        - '6h' or '6H': Every 6 hours  
        - '12h' or '12H': Every 12 hours
        - '1d' or '1D': Daily
        Any valid pandas frequency string is supported
    time_dim : str, optional
        Name of time dimension (default: 'time')
    
    Returns:
    --------
    xr.Dataset : Dataset with time steps subset at specified interval (lazy)
    
    Notes:
    ------
    - Maintains lazy evaluation - does NOT load data into memory
    - Uses index-based selection compatible with Dask arrays
    - Preserves all data variables and attributes
    - Handles both cftime and numpy datetime64 time coordinates
    - Different from temporal averaging - this selects specific time steps
      rather than averaging values
    - Automatically detects input time resolution from data
    
    Examples:
    ---------
    >>> import xarray as xr
    >>> from src.preprocessing import subset_time_by_interval
    >>> 
    >>> # Load ERA5 hourly data
    >>> ds = xr.open_dataset('era5_hourly.nc')
    >>> 
    >>> # Keep only 3-hourly time steps (00:00, 03:00, 06:00, ...)
    >>> ds_3h = subset_time_by_interval(ds, '3h')
    >>> 
    >>> # Keep only 6-hourly time steps (00:00, 06:00, 12:00, 18:00)
    >>> ds_6h = subset_time_by_interval(ds, '6h')
    >>> 
    >>> # Keep only daily time steps (00:00 each day)
    >>> ds_daily = subset_time_by_interval(ds, '1d')
    """
    logger.info(f"Subsetting time steps by interval: '{time_subset}'")
    original_times = ds.sizes[time_dim]
    
    # Parse the interval string to get target interval (e.g., '3h' -> 3 hours)
    import re
    match = re.match(r'(\d+)([a-zA-Z]+)', time_subset)
    if not match:
        raise ValueError(f"Invalid time_subset format: '{time_subset}'. Expected format like '3h', '6H', '1d'")
    
    interval_value = int(match.group(1))
    interval_unit = match.group(2).lower()
    
    # Convert target interval to timedelta
    if interval_unit in ['h', 'hour', 'hours']:
        target_interval = pd.Timedelta(hours=interval_value)
    elif interval_unit in ['d', 'day', 'days']:
        target_interval = pd.Timedelta(days=interval_value)
    elif interval_unit in ['min', 'minute', 'minutes']:
        target_interval = pd.Timedelta(minutes=interval_value)
    else:
        raise ValueError(f"Unsupported time interval unit: '{interval_unit}'. Supported: min, h, d")
    
    # Detect actual time step from data (difference between first two time steps)
    if original_times < 2:
        raise ValueError("Dataset must have at least 2 time steps to detect time resolution")
    
    # Get first two time values without loading entire array
    # Handle both cftime and numpy datetime64
    time_coord = ds[time_dim]
    time_val_0 = time_coord.isel({time_dim: 0}).values.item()
    time_val_1 = time_coord.isel({time_dim: 1}).values.item()
    
    # Convert to pandas Timestamp using helper function
    time_0 = _cftime_to_timestamp(time_val_0)
    time_1 = _cftime_to_timestamp(time_val_1)
    
    actual_timestep = time_1 - time_0
    
    logger.info(f"Detected input time step: {actual_timestep}")
    logger.info(f"Target subsetting interval: {target_interval}")
    
    # Calculate how many time steps to skip
    step_size = int(target_interval / actual_timestep)
    
    if step_size < 1:
        raise ValueError(
            f"Target interval ({target_interval}) is smaller than input time step ({actual_timestep}). "
            f"Cannot subset to finer resolution than input data."
        )
    
    if target_interval % actual_timestep != pd.Timedelta(0):
        logger.warning(
            f"Target interval ({target_interval}) is not an exact multiple of input time step ({actual_timestep}). "
            f"Will select every {step_size} time steps (≈ {step_size * actual_timestep})."
        )
    
    logger.info(f"Selecting every {step_size} time step(s) from {original_times} total")
    
    # Use index-based selection to maintain lazy evaluation
    # This is compatible with Dask and doesn't load data into memory
    time_indices = range(0, original_times, step_size)
    ds_subset = ds.isel({time_dim: list(time_indices)})
    
    subset_times = ds_subset.sizes[time_dim]
    
    logger.info(f"Time steps: {original_times} → {subset_times} ({subset_times/original_times*100:.1f}% retained)")
    logger.info(f"Reduction: {original_times - subset_times} time steps removed")
    logger.info(f"Selected time indices: {min(time_indices)} to {max(time_indices)} (every {step_size})")
    
    # Log first and last time values (without loading full array)
    first_time = ds_subset[time_dim].isel({time_dim: 0}).values
    last_time = ds_subset[time_dim].isel({time_dim: -1}).values
    logger.info(f"New time range: {first_time} to {last_time}")
    
    # Add attribute to track the subsetting
    ds_subset.attrs['time_subset_interval'] = time_subset
    ds_subset.attrs['time_subset_note'] = f'Time dimension subset to {time_subset} intervals using index selection'
    
    return ds_subset


def apply_quality_mask(ds, quality_var='quality_flag', quality_threshold=0.8, mask_value=None):
    """
    Apply quality masking to dataset based on a quality flag variable.
    
    This is an example preprocessing function that demonstrates how to create
    custom preprocessing steps for specific datasets.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with quality flag variable
    quality_var : str, optional
        Name of the quality flag variable (default: 'quality_flag')
    quality_threshold : float, optional
        Minimum quality threshold (0-1 scale, default: 0.8)
        Data below this threshold will be masked
    mask_value : float, optional
        Value to use for masked data (default: None for NaN)
    
    Returns:
    --------
    xr.Dataset : Dataset with quality mask applied to all data variables
    
    Examples:
    ---------
    >>> from src.preprocessing import apply_quality_mask
    >>> 
    >>> # Mask data with quality < 0.8
    >>> ds_filtered = apply_quality_mask(ds, quality_threshold=0.8)
    >>> 
    >>> # Use custom quality variable name
    >>> ds_filtered = apply_quality_mask(ds, quality_var='QC_flag', quality_threshold=0.5)
    """
    logger.info(f"Applying quality mask: {quality_var} >= {quality_threshold}")
    
    if quality_var not in ds:
        logger.warning(f"Quality variable '{quality_var}' not found in dataset. Skipping quality masking.")
        return ds
    
    # Create quality mask
    quality_mask = ds[quality_var] >= quality_threshold
    original_valid = quality_mask.sum().values
    
    # Apply mask to all data variables (except the quality variable itself)
    ds_masked = ds.copy()
    for var in ds.data_vars:
        if var != quality_var:
            if mask_value is None:
                ds_masked[var] = ds[var].where(quality_mask)
            else:
                ds_masked[var] = ds[var].where(quality_mask, mask_value)
    
    logger.info(f"Quality masking applied: {original_valid} points passed threshold")
    
    return ds_masked


def subset_by_region(ds, lat_bounds=None, lon_bounds=None):
    """
    Subset dataset to a geographic region.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with lat/lon coordinates
    lat_bounds : tuple of (min, max), optional
        Latitude bounds (default: None, no subsetting)
    lon_bounds : tuple of (min, max), optional
        Longitude bounds (default: None, no subsetting)
    
    Returns:
    --------
    xr.Dataset : Dataset subset to specified region
    
    Examples:
    ---------
    >>> from src.preprocessing import subset_by_region
    >>> 
    >>> # Subset to tropical region
    >>> ds_tropics = subset_by_region(ds, lat_bounds=(-30, 30))
    >>> 
    >>> # Subset to specific region
    >>> ds_region = subset_by_region(ds, lat_bounds=(20, 50), lon_bounds=(-120, -70))
    """
    logger.info(f"Subsetting dataset by region: lat={lat_bounds}, lon={lon_bounds}")
    
    ds_subset = ds
    
    if lat_bounds is not None:
        lat_min, lat_max = lat_bounds
        ds_subset = ds_subset.sel(lat=slice(lat_min, lat_max))
        logger.info(f"Latitude subset: {lat_min} to {lat_max}")
    
    if lon_bounds is not None:
        lon_min, lon_max = lon_bounds
        ds_subset = ds_subset.sel(lon=slice(lon_min, lon_max))
        logger.info(f"Longitude subset: {lon_min} to {lon_max}")
    
    logger.info(f"Dataset shape after regional subsetting: {dict(ds_subset.sizes)}")
    
    return ds_subset
