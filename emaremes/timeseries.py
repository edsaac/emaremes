import gzip

from pathlib import Path
from multiprocessing import Pool
from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr

from shapely.geometry import Point
from shapely.affinity import translate

from .utils import Extent


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


def extract_point_series(files: list[Path], lat: float, lon: float) -> pd.DataFrame:
    with Pool() as pool:
        query = pool.starmap(extract_point_value, [(f, lat, lon) for f in files])

    df = pd.DataFrame(
        {
            "timestamp": [q[0] for q in query],
            "value": [q[1] for q in query],
        },
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["value"] = df["value"].astype(float)
    df.set_index("timestamp", inplace=True)

    return df


def _extract_mean_polygon(file: Path, mask: np.ndarray, extent: Extent):
    with xr.open_dataset(
        file,
        engine="cfgrib",
        decode_timedelta=True,
    ) as ds:
        # Open file and do a coarse clip
        time = ds.time.values.copy()
        xclip = ds.loc[extent.as_xr_slice()]
        mask_ds = xclip.where(mask)

        # Actually access the files and extract the data
        mean_precip = mask_ds["unknown"].mean(dim=["longitude", "latitude"])
        metric = mean_precip.values.copy()

        return time, metric


def extract_polygon_series(files: list[Path], polygon: gpd.GeoSeries) -> pd.DataFrame:
    if len(polygon) != 1:
        raise ValueError("Only one polygon is supported")

    # Figure out the extent of first clip
    polygon_reproj = polygon.to_crs("4326")
    bounds = polygon_reproj.bounds
    extent = Extent((bounds.miny[0], bounds.maxy[0]), (bounds.minx[0], bounds.maxx[0]))

    translated_polygon = translate(polygon_reproj.geometry[0], xoff=360)

    # Generate mask from first GRIB file
    with xr.open_dataset(files[0], engine="cfgrib", decode_timedelta=True) as ds:
        xclip = ds.loc[extent.as_xr_slice()]

        # Generate points to evaluate
        lon, lat = xclip.longitude, xclip.latitude
        llon, llat = np.meshgrid(lon, lat)
        points = np.vstack((llon.flatten(), llat.flatten())).T

        # Mask using the polygon.contains calculation
        mask = [translated_polygon.contains(Point(x, y)) for x, y in points]
        mask = np.array(mask).reshape(len(lat), len(lon))

    # Query all GRIB files
    with Pool() as pool:
        query = pool.starmap(_extract_mean_polygon, [(f, mask, extent) for f in files])

    df = pd.DataFrame(
        {
            "timestamp": [q[0] for q in query],
            "value": [q[1] for q in query],
        }
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["value"] = df["value"].astype(float)
    df.set_index("timestamp", inplace=True)

    return df
