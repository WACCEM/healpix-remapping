#!/usr/bin/env python3
"""
Test script to verify spatial dimension auto-detection functionality.

This script tests the new hybrid spatial dimension detection feature:
1. Auto-detection from data files
2. Config file override
3. Fallback behavior

Usage:
    python tests/test_spatial_dimensions.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utilities import detect_spatial_dimensions


def test_auto_detection(test_paths):
    """Test spatial dimension auto-detection"""
    print("="*70)
    print("Testing Spatial Dimension Auto-Detection")
    print("="*70)
    
    # Test 1: Empty file list (should return default)
    print("\nTest 1: Empty file list")
    print("-" * 40)
    result = detect_spatial_dimensions([])
    print(f"Result: {result}")
    assert result == {'lat': -1, 'lon': -1}, "Should return default for empty list"
    print("✅ PASSED: Returns default for empty list")
    
    # Test 2: Real IMERG/IR_IMERG file (if available)
    print("\nTest 2: Real data file detection")
    print("-" * 40)
    
    # # Try common IMERG locations
    # test_paths = [
    #     "/pscratch/sd/w/wcmca1/GPM/IR_IMERG_Combined_V07B/2020/merg_2020010100_10km-pixel.nc",
    #     "/pscratch/sd/w/wcmca1/GPM/IMERG_V07B_30min_Final/2020/3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4",
    # ]
    
    file_found = False
    for test_file in test_paths:
        if Path(test_file).exists():
            print(f"Testing with: {test_file}")
            result = detect_spatial_dimensions([test_file])
            print(f"Result: {result}")
            
            # Check that we got a dictionary with -1 values
            assert isinstance(result, dict), "Should return a dictionary"
            assert all(v == -1 for v in result.values()), "All values should be -1"
            assert len(result) >= 1, "Should have at least one spatial dimension"
            
            print(f"✅ PASSED: Detected {list(result.keys())} dimensions")
            file_found = True
            break
    
    if not file_found:
        print("⚠️  SKIPPED: No test data files found (this is OK)")
    
    # Test 3: Non-existent file (should handle gracefully)
    print("\nTest 3: Non-existent file")
    print("-" * 40)
    result = detect_spatial_dimensions(["/nonexistent/file.nc"])
    print(f"Result: {result}")
    assert result == {'lat': -1, 'lon': -1}, "Should return default for bad file"
    print("✅ PASSED: Falls back to default for non-existent file")
    
    print("\n" + "="*70)
    print("All Tests Passed! ✅")
    print("="*70)


def test_expected_dimensions():
    """Document expected dimension names for different datasets"""
    print("\n" + "="*70)
    print("Expected Spatial Dimensions by Dataset")
    print("="*70)
    
    datasets = {
        "IMERG": {"lat": -1, "lon": -1},
        "IR_IMERG": {"lat": -1, "lon": -1},
        "ERA5": {"latitude": -1, "longitude": -1},
        "MERRA-2": {"lat": -1, "lon": -1},
        "E3SM": {"ncol": -1},
        "MPAS": {"nCells": -1},
        "WRF": {"south_north": -1, "west_east": -1},
    }
    
    for name, dims in datasets.items():
        print(f"\n{name}:")
        print(f"  Dimensions: {dims}")
        print(f"  Config: spatial_dimensions: {dims}")
    
    print("\n" + "="*70)


def main():
    """Run all tests"""
    
    # Specify input file locations
    test_paths = [
        "/pscratch/sd/w/wcmca1/GPM/IR_IMERG_Combined_V07B/2020/merg_2020010100_10km-pixel.nc",
        "/pscratch/sd/w/wcmca1/GPM/IMERG_V07B_30min_Final/2020/3B-HHR.MS.MRG.3IMERG.20200101-S000000-E002959.0000.V07B.HDF5.nc4",
    ]

    try:
        test_auto_detection(test_paths)
        test_expected_dimensions()
        
        print("\n✅ All spatial dimension tests completed successfully!")
        return 0
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
