#!/usr/bin/env python3
"""
Test script to compare lat/lon coordinates between two input files
"""

import numpy as np
import xarray as xr

def compare_coordinates():
    """Compare lat/lon coordinates between the two input files"""
    
    # File paths
    script_file = "/pscratch/sd/w/wcmca1/GPM/IMERG_V07B/3B-HHR.MS.MRG.3IMERG.20190101-S000000-E002959.0000.V07B.HDF5.SUB.nc4"
    notebook_file = "/pscratch/sd/w/wcmca1/GPM/IR_IMERG_Combined_V06B/2019/merg_2019010100_4km-pixel.nc"
    
    print("Comparing coordinates between input files...")
    print(f"Script file: {script_file}")
    print(f"Notebook file: {notebook_file}")
    
    # Load both datasets
    try:
        ds_script = xr.open_dataset(script_file)
        print(f"\nScript dataset loaded successfully")
        print(f"  Dimensions: {dict(ds_script.sizes)}")
        print(f"  Variables: {list(ds_script.data_vars)}")
        print(f"  Coordinates: {list(ds_script.coords)}")
        
        # Check coordinate info
        if 'lon' in ds_script.coords:
            print(f"  Longitude shape: {ds_script.lon.shape}")
            print(f"  Longitude range: {ds_script.lon.min().values:.6f} to {ds_script.lon.max().values:.6f}")
            print(f"  Longitude resolution: ~{np.diff(ds_script.lon.values).mean():.6f}°")
        if 'lat' in ds_script.coords:
            print(f"  Latitude shape: {ds_script.lat.shape}")
            print(f"  Latitude range: {ds_script.lat.min().values:.6f} to {ds_script.lat.max().values:.6f}")
            print(f"  Latitude resolution: ~{np.diff(ds_script.lat.values).mean():.6f}°")
            
    except FileNotFoundError:
        print(f"\nScript file not found: {script_file}")
        return
    except Exception as e:
        print(f"\nError loading script file: {e}")
        return
    
    try:
        ds_notebook = xr.open_dataset(notebook_file)
        print(f"\nNotebook dataset loaded successfully")
        print(f"  Dimensions: {dict(ds_notebook.sizes)}")
        print(f"  Variables: {list(ds_notebook.data_vars)}")
        print(f"  Coordinates: {list(ds_notebook.coords)}")
        
        # Check coordinate info
        if 'lon' in ds_notebook.coords:
            print(f"  Longitude shape: {ds_notebook.lon.shape}")
            print(f"  Longitude range: {ds_notebook.lon.min().values:.6f} to {ds_notebook.lon.max().values:.6f}")
            print(f"  Longitude resolution: ~{np.diff(ds_notebook.lon.values).mean():.6f}°")
        if 'lat' in ds_notebook.coords:
            print(f"  Latitude shape: {ds_notebook.lat.shape}")
            print(f"  Latitude range: {ds_notebook.lat.min().values:.6f} to {ds_notebook.lat.max().values:.6f}")
            print(f"  Latitude resolution: ~{np.diff(ds_notebook.lat.values).mean():.6f}°")
            
    except FileNotFoundError:
        print(f"\nNotebook file not found: {notebook_file}")
        return
    except Exception as e:
        print(f"\nError loading notebook file: {e}")
        return
    
    # Compare coordinates
    print(f"\n" + "="*60)
    print("COORDINATE COMPARISON")
    print("="*60)
    
    # Check if both have lon/lat coordinates
    if 'lon' in ds_script.coords and 'lon' in ds_notebook.coords:
        script_lon = ds_script.lon.values
        notebook_lon = ds_notebook.lon.values
        
        print(f"\nLONGITUDE:")
        print(f"  Script shape:   {script_lon.shape}")
        print(f"  Notebook shape: {notebook_lon.shape}")
        
        if script_lon.shape == notebook_lon.shape:
            if np.allclose(script_lon, notebook_lon, rtol=1e-10):
                print(f"  ✓ Longitude values are IDENTICAL")
            else:
                print(f"  ✗ Longitude values DIFFER")
                diff_max = np.abs(script_lon - notebook_lon).max()
                print(f"    Max difference: {diff_max}")
                print(f"    First few script values:   {script_lon[:5]}")
                print(f"    First few notebook values: {notebook_lon[:5]}")
        else:
            print(f"  ✗ Longitude shapes are DIFFERENT")
    else:
        print(f"\nLONGITUDE: Missing in one or both datasets")
    
    if 'lat' in ds_script.coords and 'lat' in ds_notebook.coords:
        script_lat = ds_script.lat.values
        notebook_lat = ds_notebook.lat.values
        
        print(f"\nLATITUDE:")
        print(f"  Script shape:   {script_lat.shape}")
        print(f"  Notebook shape: {notebook_lat.shape}")
        
        if script_lat.shape == notebook_lat.shape:
            if np.allclose(script_lat, notebook_lat, rtol=1e-10):
                print(f"  ✓ Latitude values are IDENTICAL")
            else:
                print(f"  ✗ Latitude values DIFFER")
                diff_max = np.abs(script_lat - notebook_lat).max()
                print(f"    Max difference: {diff_max}")
                print(f"    First few script values:   {script_lat[:5]}")
                print(f"    First few notebook values: {notebook_lat[:5]}")
        else:
            print(f"  ✗ Latitude shapes are DIFFERENT")
    else:
        print(f"\nLATITUDE: Missing in one or both datasets")
    
    # Final assessment
    print(f"\n" + "="*60)
    print("FINAL ASSESSMENT")
    print("="*60)
    
    coords_match = True
    if 'lon' in ds_script.coords and 'lon' in ds_notebook.coords:
        if ds_script.lon.shape != ds_notebook.lon.shape or not np.allclose(ds_script.lon.values, ds_notebook.lon.values, rtol=1e-10):
            coords_match = False
    
    if 'lat' in ds_script.coords and 'lat' in ds_notebook.coords:
        if ds_script.lat.shape != ds_notebook.lat.shape or not np.allclose(ds_script.lat.values, ds_notebook.lat.values, rtol=1e-10):
            coords_match = False
    
    if coords_match:
        print("✓ Coordinates are IDENTICAL - weight files should be the same")
        print("  The visualization differences must be due to other factors.")
    else:
        print("✗ Coordinates are DIFFERENT - this explains the different weight files!")
        print("  The script and notebook are using different input grids,")
        print("  which generates different remapping weights and results.")
        print("\n  SOLUTION: Use the same input file for both script and notebook")
        print("  to ensure identical remapping results.")

if __name__ == "__main__":
    compare_coordinates()
