#!/usr/bin/env python3

"""
Process CCIC AWS Zarr data directly to HEALPix Zarr.

Reads CCIC lazily from AWS S3 (no local download),
regrids to HEALPix,
and writes local Zarr output.

Example:
    python launch_ccic_processing.py 2021-01-01 2021-01-31 -z 9

"""

import sys
import yaml
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import xarray as xr
import ccic
import s3fs
import numpy as np
import warnings
warnings.filterwarnings("ignore")
# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from remap_to_healpix import process_to_healpix_zarr
from src.utilities import parse_date
from src.preprocessing import subset_time_by_minute

import zarr
print('zarr version:   ', zarr.__version__, flush = True )

# ============================================================
# CONFIG
# ============================================================

AWS_BUCKET = "s3://chalmerscloudiceclimatology/record/cpcir"

# ============================================================
# Config loader
# ============================================================

def load_config(config_path):

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ============================================================
# Argument parser
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description="Process CCIC data to HEALPix"
    )

    parser.add_argument(
        "start_date",
        type=str,
    )

    parser.add_argument(
        "end_date",
        type=str,
    )

    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default=None,
    )

    parser.add_argument(
        "-z",
        "--zoom",
        type=int,
        default=None,
    )

    parser.add_argument(
        "-t",
        "--time-average",
        type=str,
        default=None,
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
    )

    parser.add_argument(
        "--time-subset",
        type=str,
        choices=["00min", "30min"],
        default=None,
    )

    return parser.parse_args()


# ============================================================
# Generate CCIC file list
# ============================================================

def generate_ccic_urls(start_date, end_date):

    times = pd.date_range(
        start=start_date,
        end=end_date,freq ='1h')

    urls = []

    for tt in times:

        yyyy = tt.strftime("%Y")

        fname = (
            f"ccic_cpcir_"
            f"{tt.strftime('%Y%m%d%H%M')}.zarr"
        )

        url = f"{AWS_BUCKET}/{yyyy}/{fname}"

        urls.append(url)

    return urls


# ============================================================
# Open CCIC dataset lazily
# ============================================================

def open_ccic_dataset(
    start_date,
    end_date,
):

    urls = generate_ccic_urls(
        start_date,
        end_date,
    )

    print(f"\nOpening {len(urls)} CCIC files lazily from AWS")
    datasets = []

    for url in urls:
        try:
            ds = xr.open_zarr(
                url,
                storage_options={"anon": True},
                consolidated=True,
                 chunks={ "time": 1,
                          "latitude": 512,
                          "longitude": 512,
                         },)
            
            # keep only required variable
            data = ds[["tiwp"]]
            print('Each file contains these timesteps: ', data.sizes["time"], data.time.values)
            datasets.append(data)
            ds.close()
            
        except Exception as e:
            print(f"Skipping missing file: {url}")
            print(e)

    if len(datasets) == 0:
        raise RuntimeError("No CCIC files found")

    print("Concatenating datasets...:", len(datasets))

    ds = xr.concat(
        datasets,
        dim="time",
    )
    print('Chunks for dataset:', ds.chunks, flush = True)    
    return ds

# ============================================================
# MAIN
# ============================================================

def main():
    args = parse_arguments()
    #start_date = parse_date(   args.start_date,     is_end_date=False,)

    #end_date = parse_date(   args.end_date, is_end_date=True,)
    start_date = str(sys.argv[1])
    end_date = str(sys.argv[2])
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = start_date.replace(hour=23, minute=59, second=59)    
    # --------------------------------------------------------
    # config
    # --------------------------------------------------------

    if args.config:

        config_path = Path(args.config)

    else:

        script_dir = Path(__file__).parent

        config_path = (
            script_dir.parent
            / "config"
            / "ccic_config.yaml"
        )

    config = load_config(config_path)

    # --------------------------------------------------------
    # output
    # --------------------------------------------------------

    output_dir = Path(config["output_base_dir"])

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    zoom = args.zoom or config["default_zoom"]

    overwrite = args.overwrite

    time_average = args.time_average

    output_basename = config.get(
        "output_basename",
        "CCIC"
    )

    if args.output:

        output_file = args.output

    else:

        date_range = (
            f"{start_date.strftime('%Y%m%d')}_"
            f"{end_date.strftime('%Y%m%d')}"
        )

        filename = (
            f"{output_basename}"
            f"_30MIN"
            f"_zoom{zoom}"
            f"_{date_range}.zarr"
        )

        output_file = str(
            output_dir / filename
        )

    # --------------------------------------------------------
    # weights
    # --------------------------------------------------------

    if "weights_file" in config and config["weights_file"]:
        weights_file = str(Path(config["weights_file"]))
    elif "weights_dir" in config:
        weights_file = str(
            Path(config["weights_dir"])
            / f"ccic_to_healpix_z{zoom}_weights.nc"
        )
    else:
        weights_file = None

    
    # if "weights_file" in config:

    #     weights_file = config["weights_file"]

    # else:

    #     weights_file = None

    # --------------------------------------------------------
    # load CCIC lazily
    # --------------------------------------------------------

    ds = open_ccic_dataset(
        start_date,
        end_date,
    )

    # --------------------------------------------------------
    # optional preprocessing
    # --------------------------------------------------------

    if args.time_subset:

        ds = subset_time_by_minute(
            ds,
            time_subset=args.time_subset,
        )

    # --------------------------------------------------------
    # rename variable if needed
    # --------------------------------------------------------

    #ds = ds.rename(
    #    {"tiwp": "iwp",})

    # --------------------------------------------------------                                                                                                                                                   
    # Make lons and lats 2D instead
    
    # --------------------------------------------------------        

#    if (ds.latitude.ndim == 1 and ds.longitude.ndim == 1):
#        lon2d, lat2d = np.meshgrid(
#        ds.longitude.values.astype("float32"),
#        ds.latitude.values.astype("float32"),)

#        ds = ds.assign_coords(
#            longitude=(
#            ("latitude", "longitude"),lon2d,
#            ), latitude=(("latitude", "longitude"),lat2d),)
        
    # --------------------------------------------------------
    # processing
    # --------------------------------------------------------

    print("\nStarting HEALPix remapping")


    process_to_healpix_zarr(
        dataset=ds,
        start_date=start_date,
        end_date=end_date,
        zoom=zoom,
        output_zarr=output_file,
        weights_file=weights_file,
        overwrite=overwrite,
        time_average=time_average,
        config=config,
    )

    print("\nDone")
    print(f"Output written to:\n{output_file}")


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":

    main()
