from pathlib import Path
from multiprocessing import Pool
import argparse
import gzip
from tempfile import NamedTemporaryFile
from typing import Literal

import pandas as pd
import xarray as xr

def _extract_point_from_grib2_file(f: Path, lat: float, lon: float) -> tuple[pd.Timestamp, float]:
    """"""
    with xr.open_dataset(f, engine="cfgrib", decode_timedelta=False) as ds:
        time = ds.time.values.copy()
        val = ds.sel(latitude=lat, longitude=lon, method="nearest")["unknown"].values.copy()
    
    return time, val

def _extract_point_from_zipped_file(f: Path, lat: float, lon: float) -> tuple[pd.Timestamp, float]:
    """"""
    with gzip.open(f, "rb") as gzip_file_in:
        with NamedTemporaryFile("ab+", suffix=".grib2") as tf:
            unzipped_bytes = gzip_file_in.read()
            tf.write(unzipped_bytes)
            time, val = _extract_point_from_grib2_file(tf.name, lat, lon)

    return time, val
    


def extract_point_value(f: Path, lat: float, lon: float) -> tuple[pd.Timestamp, float]:
    
    if f.suffix == ".grib2":
        time, val = _extract_point_from_grib2_file(f, lat, lon)

    elif f.suffix == ".gz":
        time, val = _extract_point_from_zipped_file(f, lat, lon)
        
    else:
        raise ValueError("File is not `.gz` nor `.grib2`")
    
    return time, val


def extract_series(files: list[Path], lat: float, lon: float) -> pd.DataFrame:
    with Pool() as pool:
        query = pool.starmap(extract_point_value, [(f, lat, lon) for f in files])

    df = pd.DataFrame(
        {
            "timestamp": pd.DatetimeIndex([q[0] for q in query]),
            "value": [q[1] for q in query],
        },
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["value"] = df["value"].astype(float)
    df.set_index("timestamp", inplace=True)

    return df


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    parser.add_argument("--data", help="Folder with data files", type=Path)
    parser.add_argument("--suffix", help="uncompressed .grib2 or zipped .gz files", 
        choices=["grib2", "gz"], type=Literal["grib", "gz"])
    parser.add_argument("--lat", help="Latitude", type=float)
    parser.add_argument("--lon", help="Longitude", type=float)

    args = parser.parse_args()

    data = args.data
    filesfmt = args.format
    lat = args.lat
    lon = args.lon if args.lon > 0 else 360 + args.lon

    if data.is_dir():
        files = Path(args.input).glob(f"*.{filesfmt}")
        files = sorted(files)

        out_folder = Path(f"./{args.input.name}")
        out_folder.mkdir(exist_ok=True)

        df = extract_series(files, lat, lon)
        df.to_parquet(f"{out_folder}/{lat:.2f}-{lon:.2f}.parquet")

    else:
        raise ValueError(f"{data} is not a folder")
