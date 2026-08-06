#!/usr/bin/env python3
"""
Check completeness of a multi-year HEALPix Zarr store written by parallel
per-year region-write tasks (see init_healpix_store.py, submit_gsmap_array.sh,
and src.zarr_tools.write_zarr_region).

Since zarr does not skip all-NaN chunks (every chunk that gets written
creates a real chunk file on disk - verified against a completed store),
chunk-file existence is a reliable, cheap way to check completeness without
reading any data.

Usage:
    python check_healpix_store.py /path/to/store.zarr --start-year 2010 --end-year 2024

    # Print ready-to-run sbatch commands for any incomplete years
    python check_healpix_store.py /path/to/store.zarr --start-year 2010 --end-year 2024 \\
        --generate-resubmit
"""

import sys
import argparse
from pathlib import Path

import pandas as pd
import xarray as xr
import zarr

# Reuse the year -> time-index-offset logic from init_healpix_store.py so the
# two scripts can never disagree about where a year's chunks live.
sys.path.insert(0, str(Path(__file__).parent))
from init_healpix_store import compute_year_offsets


def get_store_layout(store_path):
    """
    Inspect the store once: decoded time axis, data variable names, and each
    variable's on-disk zarr shape/chunks (read via the low-level zarr API,
    which is unambiguous regardless of xarray's encoding-key conventions).

    Returns:
    --------
    dict : {
        'time': np.ndarray (decoded datetime64),
        'variables': {var_name: {'shape': (n_time, n_cell), 'chunks': (tc, sc)}}
    }
    """
    ds = xr.open_zarr(store_path, consolidated=True)
    time_values = ds['time'].values
    variables = {}
    for var_name in ds.data_vars:
        z = zarr.open(str(Path(store_path) / var_name), mode='r')
        variables[var_name] = {'shape': z.shape, 'chunks': z.chunks}
    ds.close()
    return {'time': time_values, 'variables': variables}


def find_missing_chunks(store_path, layout, offsets, start_year, end_year):
    """
    For each requested year and each data variable, check that every
    expected chunk file exists on disk.

    Returns:
    --------
    dict : {year: {var_name: [(t_chunk, c_chunk), ...missing...]}}
           Years/vars with no missing chunks are omitted.
    """
    store_path = Path(store_path)
    missing = {}

    for year in range(start_year, end_year + 1):
        if year not in offsets:
            continue  # outside the store's time axis
        i0, i1 = offsets[year]

        for var_name, info in layout['variables'].items():
            time_chunk, spatial_chunk = info['chunks']
            n_cell = info['shape'][1]
            n_spatial_chunks = -(-n_cell // spatial_chunk)  # ceil division

            # Both i0 and i1 are chunk-aligned by construction (enforced at
            # init/write time), so this is an exact, non-overlapping range.
            t_chunk_start = i0 // time_chunk
            t_chunk_end = i1 // time_chunk  # exclusive

            year_missing = []
            for t in range(t_chunk_start, t_chunk_end):
                for c in range(n_spatial_chunks):
                    chunk_file = store_path / var_name / f"{t}.{c}"
                    if not chunk_file.exists():
                        year_missing.append((t, c))

            if year_missing:
                missing.setdefault(year, {})[var_name] = year_missing

    return missing


def parse_args():
    parser = argparse.ArgumentParser(
        description='Check completeness of a parallel-written, multi-year HEALPix Zarr store.'
    )
    parser.add_argument('store', type=str, help='Path to the Zarr store')
    parser.add_argument('--start-year', type=int, required=True,
                        help='First year (inclusive) to check')
    parser.add_argument('--end-year', type=int, required=True,
                        help='Last year (inclusive) to check')
    parser.add_argument('--array-start-year', type=int, default=None,
                        help='START_YEAR used when the SLURM array was submitted, for computing '
                             'resubmit array indices (default: same as --start-year)')
    parser.add_argument('--generate-resubmit', action='store_true',
                        help='Print sbatch command(s) to resubmit incomplete years')
    return parser.parse_args()


def main():
    args = parse_args()
    store_path = Path(args.store)

    if not store_path.exists():
        print(f"ERROR: Store not found: {store_path}")
        return 1

    print(f"Checking store completeness: {store_path}")
    print(f"Year range: {args.start_year}-{args.end_year}")
    print()

    layout = get_store_layout(store_path)
    # compute_year_offsets expects a pandas-like object with a .year
    # attribute - wrap the store's decoded numpy datetime64 axis.
    offsets = compute_year_offsets(pd.DatetimeIndex(layout['time']))

    print(f"Store time axis: {len(layout['time'])} steps, "
          f"{layout['time'][0]} to {layout['time'][-1]}")
    print(f"Variables: {list(layout['variables'].keys())}")
    for var_name, info in layout['variables'].items():
        print(f"  {var_name}: shape={info['shape']}, chunks={info['chunks']}")
    print()

    missing = find_missing_chunks(store_path, layout, offsets, args.start_year, args.end_year)

    total_years = args.end_year - args.start_year + 1
    complete_years = total_years - len(missing)

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Years checked: {total_years}")
    print(f"Complete: {complete_years} ({complete_years / total_years * 100:.1f}%)")
    print(f"Incomplete: {len(missing)} ({len(missing) / total_years * 100:.1f}%)")
    print()

    if missing:
        print("=" * 70)
        print(f"INCOMPLETE YEARS ({len(missing)})")
        print("=" * 70)
        for year in sorted(missing):
            for var_name, chunks in missing[year].items():
                print(f"❌ {year} / {var_name}: {len(chunks)} missing chunk(s)")
                if len(chunks) <= 5:
                    print(f"    {chunks}")
                else:
                    print(f"    first 5: {chunks[:5]} ...")
        print()

    if args.generate_resubmit and missing:
        array_start_year = args.array_start_year or args.start_year
        indices = sorted(year - array_start_year for year in missing)
        print("=" * 70)
        print("RESUBMIT COMMAND")
        print("=" * 70)
        array_spec = ",".join(str(i) for i in indices)
        print(f"sbatch --export=STORE={store_path},START_YEAR={array_start_year} \\")
        print(f"    --array={array_spec} submit_gsmap_array.sh")
        print()

    if missing:
        return 1
    else:
        print("✅ Store is complete - all expected chunks present for all years/variables!")
        print("\nDon't forget to (re)consolidate metadata if this is the final check:")
        print(f"  python -c \"import zarr; zarr.consolidate_metadata('{store_path}')\"")
        return 0


if __name__ == '__main__':
    sys.exit(main())
