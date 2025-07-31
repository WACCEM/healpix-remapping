#!/usr/bin/env python3
"""
Chunking strategy calculator for IMERG to HEALPix remapping on Perlmutter.
This script helps determine optimal chunk sizes based on available memory and data size.
"""

import healpix as hp
import numpy as np
from pathlib import Path
import glob


def calculate_memory_usage(n_files, zoom, time_chunk, spatial_chunk, 
                          lat_size=1800, lon_size=3600, bytes_per_float=4):
    """
    Calculate memory usage for different chunking strategies.
    
    Parameters:
    -----------
    n_files : int
        Number of files to process
    zoom : int
        HEALPix zoom level
    time_chunk : int
        Time chunk size
    spatial_chunk : int
        Spatial chunk size for HEALPix
    lat_size, lon_size : int
        Original grid dimensions
    bytes_per_float : int
        Bytes per float32 value
        
    Returns:
    --------
    dict : Memory usage breakdown
    """
    nside = hp.order2nside(zoom)
    npix = hp.nside2npix(nside)
    
    # Input data memory (lat/lon grid)
    input_chunk_mb = time_chunk * lat_size * lon_size * bytes_per_float / 1e6
    
    # Output data memory (HEALPix grid)  
    output_chunk_mb = time_chunk * spatial_chunk * bytes_per_float / 1e6
    
    # Weights memory (for entire grid)
    weights_mb = npix * 3 * bytes_per_float / 1e6  # src_idx, weights, valid
    
    # Total per-worker memory
    total_per_worker_mb = input_chunk_mb + output_chunk_mb + weights_mb
    
    return {
        'input_chunk_mb': input_chunk_mb,
        'output_chunk_mb': output_chunk_mb,
        'weights_mb': weights_mb,
        'total_per_worker_mb': total_per_worker_mb,
        'npix': npix,
        'compression_ratio': (lat_size * lon_size) / npix
    }


def recommend_chunking(n_files, zoom, available_memory_gb=60):
    """
    Recommend optimal chunking strategy.
    
    Parameters:
    -----------
    n_files : int
        Number of files to process
    zoom : int
        HEALPix zoom level
    available_memory_gb : float
        Available memory per worker in GB
        
    Returns:
    --------
    dict : Recommended chunk sizes
    """
    available_memory_mb = available_memory_gb * 1000
    
    # Start with reasonable defaults
    time_chunks = [24, 48, 96, 168]  # 1 day, 2 days, 4 days, 1 week
    
    nside = hp.order2nside(zoom)
    npix = hp.nside2npix(nside)
    
    # Calculate spatial chunk options
    spatial_chunks = [
        npix // 16,    # 16 spatial chunks
        npix // 8,     # 8 spatial chunks  
        npix // 4,     # 4 spatial chunks
        npix // 2,     # 2 spatial chunks
        npix           # 1 spatial chunk
    ]
    
    best_combo = None
    best_throughput = 0
    
    recommendations = []
    
    for time_chunk in time_chunks:
        for spatial_chunk in spatial_chunks:
            mem_usage = calculate_memory_usage(n_files, zoom, time_chunk, spatial_chunk)
            
            if mem_usage['total_per_worker_mb'] < available_memory_mb:
                # Estimate throughput (arbitrary units)
                throughput = time_chunk * spatial_chunk / mem_usage['total_per_worker_mb']
                
                recommendations.append({
                    'time_chunk': time_chunk,
                    'spatial_chunk': spatial_chunk,
                    'memory_mb': mem_usage['total_per_worker_mb'],
                    'throughput_score': throughput,
                    'memory_efficiency': mem_usage['total_per_worker_mb'] / available_memory_mb
                })
                
                if throughput > best_throughput:
                    best_throughput = throughput
                    best_combo = {
                        'time_chunk': time_chunk,
                        'spatial_chunk': spatial_chunk,
                        'memory_usage': mem_usage
                    }
    
    return best_combo, recommendations


def estimate_processing_time(n_files, zoom, time_chunk, n_workers=8):
    """
    Estimate processing time based on chunk sizes.
    
    Parameters:
    -----------
    n_files : int
        Number of files
    zoom : int  
        HEALPix zoom level
    time_chunk : int
        Time chunk size
    n_workers : int
        Number of Dask workers
        
    Returns:
    --------
    dict : Time estimates
    """
    # Assume each file has 48 time steps (30min intervals for 1 day)
    total_time_steps = n_files * 48
    n_time_chunks = np.ceil(total_time_steps / time_chunk)
    
    # Rough estimates based on zoom level complexity
    seconds_per_chunk = {
        7: 2,    # ~50k pixels
        8: 5,    # ~200k pixels  
        9: 15,   # ~800k pixels
        10: 45,  # ~3M pixels
        11: 120  # ~12M pixels
    }.get(zoom, 60)
    
    total_chunks = n_time_chunks
    parallel_time_hours = (total_chunks * seconds_per_chunk) / (n_workers * 3600)
    
    return {
        'total_time_steps': total_time_steps,
        'n_time_chunks': n_time_chunks,
        'estimated_hours': parallel_time_hours,
        'estimated_days': parallel_time_hours / 24
    }


def print_recommendations(start_year, end_year, zoom):
    """Print chunking recommendations for given parameters."""
    
    # Estimate number of files
    files_per_year = 365 * 48  # Assuming 48 files per day (30min intervals)
    n_years = end_year - start_year + 1
    n_files = n_years * files_per_year
    
    print(f"\n=== IMERG to HEALPix Chunking Recommendations ===")
    print(f"Period: {start_year}-{end_year} ({n_years} years)")
    print(f"HEALPix zoom: {zoom}")
    print(f"Estimated files: {n_files:,}")
    
    nside = hp.order2nside(zoom)
    npix = hp.nside2npix(nside)
    print(f"HEALPix pixels: {npix:,}")
    
    # Get recommendations
    best_combo, all_recommendations = recommend_chunking(n_files, zoom)
    
    if best_combo:
        print(f"\n=== RECOMMENDED CONFIGURATION ===")
        print(f"Time chunk size: {best_combo['time_chunk']} time steps")
        print(f"Spatial chunk size: {best_combo['spatial_chunk']:,} HEALPix cells")
        print(f"Memory per worker: {best_combo['memory_usage']['total_per_worker_mb']:.1f} MB")
        print(f"Compression ratio: {best_combo['memory_usage']['compression_ratio']:.2f}x")
        
        # Time estimate
        time_est = estimate_processing_time(
            n_files, zoom, best_combo['time_chunk']
        )
        print(f"\n=== TIME ESTIMATES ===")
        print(f"Total time chunks: {time_est['n_time_chunks']:,}")
        print(f"Estimated processing time: {time_est['estimated_hours']:.1f} hours")
        print(f"                         ({time_est['estimated_days']:.1f} days)")
        
    else:
        print("\nWARNING: No suitable chunking strategy found!")
        print("Consider reducing the zoom level or the time period.")
    
    print(f"\n=== ALL OPTIONS (sorted by efficiency) ===")
    sorted_recs = sorted(all_recommendations, key=lambda x: x['throughput_score'], reverse=True)
    print(f"{'Time':<6} {'Spatial':<10} {'Memory (MB)':<12} {'Efficiency':<10} {'Score':<8}")
    print("-" * 60)
    
    for rec in sorted_recs[:10]:  # Show top 10
        print(f"{rec['time_chunk']:<6} {rec['spatial_chunk']:<10,} "
              f"{rec['memory_mb']:<12.1f} {rec['memory_efficiency']:<10.2f} "
              f"{rec['throughput_score']:<8.1f}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python chunking_calculator.py <start_year> <end_year> <zoom>")
        print("Example: python chunking_calculator.py 2019 2021 9")
        sys.exit(1)
    
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    zoom = int(sys.argv[3])
    
    print_recommendations(start_year, end_year, zoom)
