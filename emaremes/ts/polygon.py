from os import PathLike
from pathlib import Path
from multiprocessing import Pool

import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr

from pyproj.crs.crs import CRS
from shapely.geometry import Point, Polygon
from shapely.affinity import translate

from ..utils import Extent, unzip_if_gz


@unzip_if_gz
def _extract_using_masks_from_file(
    file: PathLike,
    masks: dict[str, np.ndarray],
    extent: Extent,
    variable: str = "unknown",
    upsample_coords: dict[str, np.ndarray] | None = None,
) -> tuple[np.datetime64, dict[str, float]]:
    """
    Extracts the values of a grib2 file provided a mask and an extent.

    Parameters
    ----------
    file : PathLike
        Path to the grib2 file.
    masks : dict[str, np.ndarray]
        Masks to apply to the grib2 file.
    extent : Extent
        Extent to clip the grib2 file to.
    variable : str, optional
        Variable to extract from the grib2 file, by default "unknown" which
        represents precipitation intensity in mm/h.
    upsample_coords : dict[str, np.ndarray] | None, optional
        Coordinates to upsample the data to, by default None.

    Returns
    -------
    tuple[np.datetime64, dict[str, float]]
        A tuple with the timestamp and values for the polygons.
    """
    with xr.open_dataset(file, engine="cfgrib", decode_timedelta=True) as ds:
        # Open file and do a coarse clip
        time = ds.time.values.copy()
        xclip = ds.loc[extent.as_xr_slice()]
        xclip = xclip.where(xclip["unknown"] != -3)

        # Upscaling helper
        if upsample_coords:
            upsample = xclip.interp(coords=upsample_coords, method="nearest")
        else:
            upsample = xclip

        # Dictionary to store the data
        data = {}

        for id, mask in masks.items():
            mask_ds = upsample.where(mask)

            # Actually access the files and extract the data
            mean_precip = mask_ds[variable].mean(dim=["longitude", "latitude"])
            data[id] = float(mean_precip.values.copy())

    return time, data


@unzip_if_gz
def _calculate_masks_and_coords(
    f: PathLike,
    polygons: dict[str, Polygon],
    extent: Extent,
    upsample: bool = True,
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """"""
    with xr.open_dataset(f, engine="cfgrib", decode_timedelta=True) as ds:
        xclip = ds.loc[extent.as_xr_slice()]
        lon, lat = xclip.longitude.values.copy(), xclip.latitude.values.copy()

        if upsample:
            llon = np.linspace(min(lon), max(lon), num=4 * len(lon) - 1)
            llat = np.linspace(min(lat), max(lat), num=4 * len(lat) - 1)

        else:
            llon, llat = lon, lat

        upsample_coords = {"longitude": llon, "latitude": llat}
        mlon, mlat = np.meshgrid(llon, llat)
        points = np.vstack((mlon.flatten(), mlat.flatten())).T

        # Mask using the polygon.contains calculation
        masks = {
            k: np.array([p.contains(Point(x, y)) for x, y in points]).reshape(len(llat), len(llon))
            for k, p in polygons.items()
        }

    return masks, upsample_coords


def query_single_file(
    file: Path,
    geodata: gpd.GeoDataFrame,
    upsample: bool = False,
) -> tuple[np.datetime64, dict[str, float]]:
    """
    Queries a single grib2 file and calculates the mean value encompassed by
    each of the polygons in a GeoDataFrame.

    Parameters
    ----------
    file : PathLike
        Path to grib2 file to extract the polygon values from.
    geodata : gpd.GeoDataFrame
        Geopandas dataframe of polygons to extract the value from.
    upsample : bool, optional
        Whether to upsample the data to a finer grid, by default False.

    Returns
    -------
    tuple[np.datetime64, dict[str, float]]
        A tuple with the timestamp and values of the queried points.
    """

    # Figure out the extent of first clip
    if geodata.crs != CRS("EPSG:4326"):
        geodata["geometry"] = geodata["geometry"].buffer(0.001)
        blob = geodata.dissolve().simplify(tolerance=50)
    else:
        blob = geodata.dissolve()

    geo_blob = blob.to_crs("4326")
    all_bounds = geo_blob.bounds
    extent = Extent(
        (all_bounds.miny[0], all_bounds.maxy[0]),
        (all_bounds.minx[0], all_bounds.maxx[0]),
    )

    # Reproject the geodatabase and create a mapping of identifier: polygon
    geodata_reproj = geodata.to_crs("4326")
    translated_polygons = {k: translate(geo.geometry, xoff=360) for k, geo in geodata_reproj.iterrows()}

    # Generate masks
    masks, upsample_coords = _calculate_masks_and_coords(file, translated_polygons, extent, upsample)

    return _extract_using_masks_from_file(file, masks, extent, "unknown", upsample_coords)


def query_files(
    files: list[Path],
    geodata: gpd.GeoDataFrame,
    upsample: bool = False,
) -> pd.DataFrame:
    """
    Parallelizes the extraction of polygon values from grib2 files. For a large number of files,
    this can be much faster than using `xr.open_mfdataset`.

    Parameters
    ----------
    files : list[Path]
        List of grib2 files to extract the polygon value from.
    geodata : gpd.GeoDataFrame
        Geopandas dataframe of polygons to extract the value from.
    upsample : bool = False
        Whether to upsample the data to a finer grid, by default False.

    Returns
    -------
    pd.Dataframe
        Pandas dataframe with the extracted values. Rows are indexed by timestamp, columns are
        identified by the indexes in the `geodata` GeoDataFrame.
    """

    # Figure out the extent of first clip
    if geodata.crs != CRS("EPSG:4326"):
        geodata["geometry"] = geodata["geometry"].buffer(0.001)
        blob = geodata.dissolve().simplify(tolerance=50)
    else:
        blob = geodata.dissolve()

    geo_blob = blob.to_crs("4326")
    all_bounds = geo_blob.bounds
    extent = Extent(
        (all_bounds.miny[0], all_bounds.maxy[0]),
        (all_bounds.minx[0], all_bounds.maxx[0]),
    )

    # Reproject the geodatabase and create a mapping of identifier: polygon
    geodata_reproj = geodata.to_crs("4326")
    translated_polygons = {k: translate(geo.geometry, xoff=360) for k, geo in geodata_reproj.iterrows()}

    # Generate masks using the first file
    masks, upsample_coords = _calculate_masks_and_coords(files[0], translated_polygons, extent, upsample)

    # Query all GRIB files
    with Pool() as pool:
        query = pool.starmap(
            _extract_using_masks_from_file,
            [(f, masks, extent, "unknown", upsample_coords) for f in files],
        )

    df = pd.DataFrame([{"timestamp": timestamp, **values} for timestamp, values in query])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df.set_index("timestamp", inplace=True)

    return df


__all__ = ["query_single_file", "query_files"]
