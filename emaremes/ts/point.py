from multiprocessing import Pool
from os import PathLike
from pathlib import Path

import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd

from ..utils import Extent, unzip_if_gz


@unzip_if_gz
def query_single_file(f: PathLike, geodata: gpd.GeoDataFrame) -> tuple[np.datetime64, dict[str, float]]:
    """
    Extracts the nearest value of a grib2 file provided a latitude and longitude.

    Parameters
    ----------
    f : PathLike
        Path to the grib2 file.
    geodata: gpd.GeoDataFrame
        GeoDataFrame containing Points as geometries.

    Returns
    -------
    tuple[np.datetime64, dict[str, float]]
        A tuple with the timestamp and values of the queried points.
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
    pd.Dataframe
        Pandas dataframe with the extracted values. Rows are indexed by timestamp, columns are
        identified by the indexes in the `geodata` GeoDataFrame.
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
