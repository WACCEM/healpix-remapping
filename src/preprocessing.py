#!/usr/bin/env python3
"""
Dataset-specific preprocessing functions.

This module contains preprocessing functions that are specific to certain datasets
(e.g., IMERG, IR_IMERG) and are not part of the generic remapping pipeline.
These functions can be applied as optional preprocessing steps before remapping.
"""

import pandas as pd
import logging

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
    ds_subset = ds.isel(time=mask)
    subset_times = ds_subset.sizes['time']
    
    logger.info(f"Time steps: {original_times} → {subset_times} ({subset_times/original_times*100:.1f}% retained)")
    logger.info(f"Reduction: {original_times - subset_times} time steps removed")
    
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
