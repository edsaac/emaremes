import gzip

from multiprocessing import Pool
from pathlib import Path
from tempfile import NamedTemporaryFile

import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd

from ..utils import Extent


def _extract_points_from_grib2_file(
    f: Path, geodata: gpd.GeoDataFrame
) -> tuple[np.datetime64, dict[str, float]]:
    """
    Extracts the nearest value of a grib2 file provided a GeoDataFrame containing
    Points as geometries.

    Parameters
    ----------
    f : Path
        Path to the grib2 file.
    geodata: gpd.GeoDataFrame
        GeoDataFrame containing Points as geometries.

    Returns
    -------
    tuple[pd.Timestamp, dict[str, float]]
        A tuple with the timestamp and value of the point.
    """
    geodata = geodata.to_crs("4326")
    bounds = geodata.total_bounds
    extent = Extent((bounds[1], bounds[3]), (bounds[0], bounds[2]))

    with xr.open_dataset(f, engine="cfgrib", decode_timedelta=False) as ds:
        # Mask out no data (-3 for precipitation data) and hide small intensities
        ds = ds.where(ds["unknown"] != -3)
        time = ds.time.values.copy()
        xclip = ds.loc[extent.as_xr_slice()]
        data = {}

        for index, point in geodata.iterrows():
            lon, lat = point.geometry.x, point.geometry.y
            lon = 360 + lon if lon < 0 else lon

            v = xclip.sel(latitude=lat, longitude=lon, method="nearest")["unknown"].values.copy()
            data[str(index)] = float(v)

    return time, data


def _extract_points_from_zipped_file(
    f: Path, geodata: gpd.GeoDataFrame
) -> tuple[np.datetime64, dict[str, float]]:
    """
    Extracts the nearest value of a gzipped grib2 file provided a latitude and longitude.
    This just deflates the file and calls `_extract_points_from_grib2_file`.

    Parameters
    ----------
    f : Path
        Path to the gzipped grib2 file.

    Returns
    -------
    tuple[pd.Timestamp, float]
        A tuple with the timestamp and value of the point.
    """
    with gzip.open(f, "rb") as gzip_file_in:
        with NamedTemporaryFile("ab+", suffix=".grib2") as tf:
            unzipped_bytes = gzip_file_in.read()
            tf.write(unzipped_bytes)
            time, data = _extract_points_from_grib2_file(tf.name, geodata)

    return time, data


def query_single_file(f: Path, geodata: gpd.GeoDataFrame) -> tuple[np.datetime64, dict[str, float]]:
    """
    Extracts the nearest value of a grib2 file provided a latitude and longitude.

    Parameters
    ----------
    f : Path
        Path to the grib2 file.
    geodata: gpd.GeoDataFrame
        GeoDataFrame containing Points as geometries.

    Returns
    -------
    tuple[np.datetime64, dict[str, float]]
        A tuple with the timestamp and values of the queried points.
    """

    f = Path(f)

    if f.suffix == ".grib2":
        return _extract_points_from_grib2_file(f, geodata)

    elif f.suffix == ".gz":
        return _extract_points_from_zipped_file(f, geodata)

    raise ValueError("File is not `.gz` nor `.grib2`")


def query_files(files: list[Path], geodata: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Parallelizes the extraction of point values from grib2 files. For a large number of files,
    this can be much faster than using `xr.open_mfdataset`.

    Parameters
    ----------
    files : list[Path]
        List of grib2 files to extract the point value from.
    geodata : gpd.GeoDataFrame
        GeoDataFrame containing Points as geometries.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame with the timestamps and values of the points in geodata.
    """
    if not files:
        raise ValueError("No files to query")

    with Pool() as pool:
        query = pool.starmap(query_single_file, [(f, geodata) for f in files])

    df = pd.DataFrame([{"timestamp": timestamp, **values} for timestamp, values in query])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df.set_index("timestamp", inplace=True)

    return df


__all__ = ["query_files", "query_single_file"]
