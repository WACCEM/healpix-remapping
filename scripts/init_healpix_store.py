#!/usr/bin/env python3
"""
Initialize an empty, chunk-aligned HEALPix Zarr store spanning a multi-year
time range, for parallel region writes by multiple independent processing
tasks (e.g. one SLURM array task per year).

Run this ONCE, interactively, before submitting the processing job array.
It writes only Zarr metadata plus the real 'time'/'cell'/'crs' coordinate
arrays - no data chunks are written (unwritten cells read back as NaN until
each task fills in its own region). Downstream tasks then run
launch_gsmap_processing.py ... --region-store <this store> to fill in their
slice, via src.zarr_tools.write_zarr_region().

Variable names, dtypes, attributes, and the 'cell'/'crs' coordinates are
derived by running the real read + remap pipeline
(utilities.read_concat_files + remap_tools.remap_delaunay) on ONE sample
day, so the skeleton is guaranteed to match exactly what the per-task jobs
will write - no hand-maintained schema to keep in sync.

Usage:
    python init_healpix_store.py -c ../config/gsmap_config.yaml \\
        --start-year 2010 --end-year 2024 -z 9 \\
        -o /pscratch/sd/w/wcmca1/GsMAP/healpix/GsMAPv8_1H_zoom9_20100101_20241231.zarr

Then submit the processing array (see submit_gsmap_array.sh), and finally
check completeness / consolidate metadata once all tasks finish:
    python check_healpix_store.py <store> --start-year 2010 --end-year 2024
    python -c "import zarr; zarr.consolidate_metadata('<store>')"
"""

import sys
import shutil
import argparse
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import xarray as xr
import zarr
import dask.array as da
import yaml

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from src import utilities, remap_tools, chunk_tools

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config(config_path):
    """Load configuration from YAML file (mirrors launch_gsmap_processing.py)."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def resolve_weights_file(config, zoom):
    """
    Determine the weights file path, mirroring the priority order used by
    launch_gsmap_processing.py: explicit config['weights_file'], then an
    auto-generated name under config['weights_dir'], then None (weights
    computed on-the-fly).
    """
    if config.get('weights_file'):
        return str(Path(config['weights_file']))
    if 'weights_dir' in config:
        return str(Path(config['weights_dir']) / f"imerg_v07b_to_healpix_z{zoom}_weights.nc")
    return None


def build_full_time_axis(start_year, end_year):
    """Hourly time axis from Jan 1 00:00 of start_year to Dec 31 23:00 of end_year."""
    start = pd.Timestamp(f"{start_year}-01-01T00:00:00")
    end = pd.Timestamp(f"{end_year}-12-31T23:00:00")
    return pd.date_range(start, end, freq='h')


def compute_year_offsets(time_axis):
    """
    Map each year present in time_axis to its [start, end) integer offset
    range, e.g. {2010: (0, 8760), 2011: (8760, 17520), ...}.
    """
    years = time_axis.year
    offsets = {}
    for year in sorted(set(years)):
        idx = np.flatnonzero(years == year)
        offsets[int(year)] = (int(idx[0]), int(idx[-1]) + 1)
    return offsets


def verify_chunk_alignment(offsets, time_chunk_size):
    """
    Confirm every year's [start, end) offset lands on a time-chunk boundary -
    the invariant that makes concurrent per-year region writes safe (see
    src.zarr_tools.write_zarr_region). Returns a list of problem descriptions
    (empty if everything is aligned).
    """
    problems = []
    for year, (i0, i1) in offsets.items():
        if i0 % time_chunk_size != 0:
            problems.append(f"{year}: start offset {i0} is not a multiple of time_chunk_size={time_chunk_size}")
        if i1 % time_chunk_size != 0:
            problems.append(f"{year}: end offset {i1} is not a multiple of time_chunk_size={time_chunk_size}")
    return problems


def check_input_file_coverage(config, offsets):
    """
    Informational pre-flight check: compare each year's expected hour count
    (from the calendar) against the number of input files actually found.
    Mismatches are only a WARNING, not a hard failure - the store's time
    axis is calendar-defined; a year with missing source files simply ends
    up with unwritten (NaN) chunks for that gap, which
    check_healpix_store.py will detect after processing.
    """
    logger.info("Checking input file coverage for each year (informational)...")
    all_ok = True
    for year, (i0, i1) in offsets.items():
        expected = i1 - i0
        year_start = datetime(year, 1, 1, 0, 0, 0)
        year_end = datetime(year, 12, 31, 23, 59, 59)
        files = utilities.get_input_files(
            year_start, year_end, config['input_base_dir'],
            date_pattern=config.get('date_pattern', r'\.(\d{8})-'),
            date_format=config.get('date_format', '%Y%m%d'),
            use_year_subdirs=config.get('use_year_subdirs', True),
            file_glob=config.get('file_glob', '*.nc*'),
        )
        status = "OK" if len(files) == expected else "MISMATCH"
        if status == "MISMATCH":
            all_ok = False
        logger.info(f"  {year}: expected {expected} hourly files, found {len(files)}  [{status}]")
    if all_ok:
        logger.info("✅ All years have complete input file coverage")
    else:
        logger.warning("⚠️  Some years have incomplete input file coverage (see MISMATCH above)")
        logger.warning("    The store will still be created; those years will have gaps until re-run")
    return all_ok


def get_sample_day_files(config, sample_date):
    """Find input files for a single day, used to derive the output schema."""
    day_start = datetime(sample_date.year, sample_date.month, sample_date.day, 0, 0, 0)
    day_end = datetime(sample_date.year, sample_date.month, sample_date.day, 23, 59, 59)
    files = utilities.get_input_files(
        day_start, day_end, config['input_base_dir'],
        date_pattern=config.get('date_pattern', r'\.(\d{8})-'),
        date_format=config.get('date_format', '%Y%m%d'),
        use_year_subdirs=config.get('use_year_subdirs', True),
        file_glob=config.get('file_glob', '*.nc*'),
    )
    if not files:
        raise ValueError(
            f"No input files found for sample date {sample_date.date()} - "
            f"pick a different --sample-date (must have data available)"
        )
    logger.info(f"Sample date {sample_date.date()}: found {len(files)} files")
    return files


def build_sample_remap(config, zoom, files, weights_file):
    """
    Run the real read + remap pipeline (the same functions
    remap_to_healpix.process_to_healpix_zarr() uses) on one sample day, to
    derive the exact variable set, dtypes, attrs, and 'cell'/'crs' coords
    the per-year jobs will produce. Nothing is computed/written to disk here
    - the remapped arrays stay lazy (dask-backed); only small coordinate
    arrays ('time', 'cell') are eagerly materialized.
    """
    concat_dim = config.get('concat_dim', 'time')
    spatial_chunks = config.get('spatial_dimensions', None)
    if spatial_chunks is None:
        logger.info("spatial_dimensions not in config - auto-detecting from sample files...")
        spatial_chunks = utilities.detect_spatial_dimensions(files, time_dim=concat_dim)

    ds = utilities.read_concat_files(
        files,
        time_chunk_size=config.get('time_chunk_size', 48),
        spatial_dims=spatial_chunks,
        concat_dim=concat_dim,
        combine_vars=config.get('combine_vars', False),
        fix_time_units=config.get('fix_time_units', False),
        rename_variables=config.get('rename_variables', None),
        use_cftime=config.get('use_cftime', True),
    )
    ds = utilities.temporal_average(ds, config.get('time_average'), config.get('convert_time', False))

    ds_remap = remap_tools.remap_delaunay(ds, zoom, weights_file, config=config)

    # Mirror remap_to_healpix.py's Step 3b (variable subsetting/renaming) so
    # the store schema matches exactly what the per-year jobs will write.
    remap_variables = config.get('remap_variables')
    if remap_variables:
        vars_to_keep = [v for v in remap_variables if v in ds_remap.data_vars]
        passthrough = [v for v in config.get('passthrough_variables', []) if v in ds_remap.data_vars]
        vars_to_keep = vars_to_keep + passthrough
        if vars_to_keep:
            ds_remap = ds_remap[vars_to_keep]
            for old_name, new_name in remap_variables.items():
                if old_name in ds_remap.data_vars:
                    ds_remap = ds_remap.rename({old_name: new_name})

    if not np.issubdtype(ds_remap['time'].dtype, np.datetime64):
        raise TypeError(
            f"init_healpix_store.py requires the source data to decode to numpy "
            f"datetime64 time values (got dtype={ds_remap['time'].dtype}), since the "
            f"full multi-year time axis is built with pandas.date_range(). This holds "
            f"for GsMAP with fix_time_units=true / use_cftime=false in config. "
            f"cftime-based datasets are not currently supported by this script."
        )

    logger.info(f"Derived schema - variables: {list(ds_remap.data_vars)}")
    for var in ds_remap.data_vars:
        logger.info(f"  {var}: dtype={ds_remap[var].dtype}, dims={ds_remap[var].dims}")
    return ds_remap


def build_template_store(ds_remap, time_axis, time_chunk_size, spatial_chunk_size):
    """
    Build the empty (lazy, NaN-filled) full-time-range dataset to write as
    the store skeleton: real 'time'/'cell'/'crs' coordinates, and correct
    dims/dtypes/attrs per variable (taken from ds_remap), but with dask
    placeholder data spanning the full time_axis instead of ds_remap's
    single sample day.
    """
    n_time = len(time_axis)
    npix = ds_remap.sizes['cell']

    data_vars = {}
    for var_name in ds_remap.data_vars:
        sample_da = ds_remap[var_name]
        store_dtype = 'float32' if sample_da.dtype.kind == 'f' else sample_da.dtype
        fill_value = np.nan if np.dtype(store_dtype).kind == 'f' else 0

        placeholder = da.full(
            (n_time, npix), fill_value,
            dtype=store_dtype,
            chunks=(time_chunk_size, spatial_chunk_size)
        )
        data_vars[var_name] = xr.DataArray(
            placeholder, dims=('time', 'cell'), attrs=dict(sample_da.attrs)
        )

    template = xr.Dataset(
        data_vars,
        coords={
            'time': ('time', time_axis.values),
            'cell': ('cell', ds_remap['cell'].values),
            'crs': ds_remap['crs'],
        },
        attrs=dict(ds_remap.attrs),
    )
    template.attrs.update({
        'store_kind': 'empty_skeleton_for_region_writes',
        'creation_note': (
            'Created by scripts/init_healpix_store.py - data chunks are unwritten '
            '(read back as NaN) until filled in by per-task region writes '
            '(see src.zarr_tools.write_zarr_region).'
        ),
        'creation_date': str(np.datetime64('now')),
    })
    return template


def build_encoding(template, time_chunk_size, spatial_chunk_size):
    """
    Zarr encoding for the skeleton write - matches
    src.zarr_tools.write_zarr_with_monitoring()'s formula exactly, so chunks
    written later by write_zarr_region() land on the same on-disk layout.
    Deliberately does NOT set encoding for 'time' (forcing a dtype there is
    known to corrupt the time coordinate - see zarr_tools.py) or 'crs'
    (scalar, defaults are fine).
    """
    encoding = {}
    for var_name in template.data_vars:
        var = template[var_name]
        encoding[var_name] = {
            'compressor': zarr.Blosc(cname='zstd', clevel=3, shuffle=2),
            'chunks': (time_chunk_size, spatial_chunk_size),
            'dtype': 'float32' if var.dtype.kind == 'f' else var.dtype,
        }
    cell_var = template.coords['cell']
    if cell_var.dtype.kind == 'i':
        encoding['cell'] = {'dtype': 'int32'}
    elif cell_var.dtype.kind == 'f':
        encoding['cell'] = {'dtype': 'float32'}
    return encoding


def parse_args():
    parser = argparse.ArgumentParser(
        description='Initialize an empty, chunk-aligned HEALPix Zarr store for parallel region writes.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -c ../config/gsmap_config.yaml --start-year 2010 --end-year 2024 -z 9 \\
      -o /pscratch/sd/w/wcmca1/GsMAP/healpix/GsMAPv8_1H_zoom9_20100101_20241231.zarr
        """
    )
    parser.add_argument('-c', '--config', type=str, required=True,
                        help='Path to config YAML file')
    parser.add_argument('--start-year', type=int, required=True,
                        help='First year (inclusive) of the store time range')
    parser.add_argument('--end-year', type=int, required=True,
                        help='Last year (inclusive) of the store time range')
    parser.add_argument('-z', '--zoom', type=int, default=None,
                        help='HEALPix zoom level (overrides config default_zoom)')
    parser.add_argument('-o', '--output', type=str, required=True,
                        help='Path for the new empty Zarr store')
    parser.add_argument('--sample-date', type=str, default=None,
                        help='Date (YYYY-MM-DD) used to derive variable schema/dtypes; '
                             'default: Jan 1 of --start-year')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite an existing store at --output')
    parser.add_argument('--skip-coverage-check', action='store_true',
                        help='Skip the informational per-year input file count check (faster)')
    return parser.parse_args()


def main():
    args = parse_args()

    if args.end_year < args.start_year:
        print(f"Error: --end-year ({args.end_year}) must be >= --start-year ({args.start_year})")
        sys.exit(1)

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    config = load_config(str(config_path))

    zoom = args.zoom or config['default_zoom']
    time_chunk_size = config.get('time_chunk_size', 48)
    spatial_chunk_size = chunk_tools.compute_chunksize(order=zoom)

    output_path = Path(args.output)
    if output_path.exists():
        if args.overwrite:
            logger.info(f"Overwriting existing store: {output_path}")
            shutil.rmtree(output_path)
        else:
            print(f"Error: Output store already exists: {output_path}")
            print("Use --overwrite to replace it, or choose a different --output path.")
            sys.exit(1)

    print("=" * 70)
    print("Initializing HEALPix region-write Zarr store")
    print("=" * 70)
    print(f"Config: {config_path}")
    print(f"Years: {args.start_year}-{args.end_year}")
    print(f"Zoom: {zoom}  |  time_chunk_size: {time_chunk_size}  |  spatial_chunk_size: {spatial_chunk_size}")
    print(f"Output: {output_path}")
    print("=" * 70)

    # 1. Build the full time axis and per-year offsets
    time_axis = build_full_time_axis(args.start_year, args.end_year)
    offsets = compute_year_offsets(time_axis)
    logger.info(f"Full time axis: {len(time_axis)} hourly steps, "
                f"{time_axis[0]} to {time_axis[-1]}")

    # 2. Verify chunk alignment BEFORE doing anything else - this is the
    # invariant the whole parallel-write design depends on.
    problems = verify_chunk_alignment(offsets, time_chunk_size)
    if problems:
        print("\nError: chunk alignment check FAILED:")
        for p in problems:
            print(f"  - {p}")
        print(f"\nEvery year must start/end on a multiple of time_chunk_size={time_chunk_size}.")
        print("This holds for whole-year ranges as long as time_chunk_size evenly divides 24")
        print("(e.g. 24, 12, 8, 6, 4, 3, 2, 1). Fix config['time_chunk_size'] and retry.")
        sys.exit(1)
    logger.info("✅ Chunk alignment check passed for all years")

    print("\nYear -> time-index range (0-indexed, end exclusive):")
    for year, (i0, i1) in offsets.items():
        print(f"  {year}: [{i0:>7}:{i1:>7}]  ({i1 - i0} steps)")

    # 3. Informational input file coverage check
    if not args.skip_coverage_check:
        check_input_file_coverage(config, offsets)

    # 4. Derive schema from one real sample day
    sample_date = (pd.Timestamp(args.sample_date) if args.sample_date
                   else pd.Timestamp(f"{args.start_year}-01-01"))
    if not (pd.Timestamp(f"{args.start_year}-01-01") <= sample_date <= pd.Timestamp(f"{args.end_year}-12-31")):
        logger.warning(f"--sample-date {sample_date.date()} is outside the store's year range "
                        f"{args.start_year}-{args.end_year} (allowed, just unusual)")

    weights_file = resolve_weights_file(config, zoom)
    logger.info(f"Weights file: {weights_file or '(none - will compute on-the-fly)'}")

    sample_files = get_sample_day_files(config, sample_date)
    ds_remap = build_sample_remap(config, zoom, sample_files, weights_file)

    # 5. Build the full-time-range template and write only its metadata +
    # coordinates (compute=False -> no data chunks written for data_vars)
    template = build_template_store(ds_remap, time_axis, time_chunk_size, spatial_chunk_size)
    encoding = build_encoding(template, time_chunk_size, spatial_chunk_size)

    logger.info(f"Template dataset: {dict(template.sizes)}")
    logger.info(f"Uncompressed size if fully written: {template.nbytes / 1024**3:.1f} GB")

    logger.info(f"Writing store skeleton (metadata + coordinates only) to {output_path}...")
    template.to_zarr(output_path, mode='w', encoding=encoding, compute=False, consolidated=True)
    logger.info("✅ Store skeleton created")

    print("\n" + "=" * 70)
    print("✅ Store initialized successfully (no data chunks written yet)")
    print(f"   {output_path}")
    print(f"   time: {len(time_axis)}  cell: {template.sizes['cell']}")
    print(f"   variables: {list(template.data_vars)}")
    print("=" * 70)
    print("\nNext steps:")
    print(f"  1. Submit the processing array (one task per year), e.g.:")
    print(f"       sbatch --export=STORE={output_path} submit_gsmap_array.sh")
    print(f"  2. After all tasks succeed, check completeness and consolidate:")
    print(f"       python check_healpix_store.py {output_path} --start-year {args.start_year} --end-year {args.end_year}")
    print(f"       python -c \"import zarr; zarr.consolidate_metadata('{output_path}')\"")


if __name__ == '__main__':
    main()
