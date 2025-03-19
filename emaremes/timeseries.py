import gzip

from pathlib import Path
from multiprocessing import Pool
from tempfile import NamedTemporaryFile

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
