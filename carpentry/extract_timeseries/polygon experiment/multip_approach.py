"""
Benchmark:
717 GRIB files (1 day @ 2min)

naive -> 5m28.752s
multip -> 0m55.990s

"""

from pathlib import Path
from multiprocessing import Pool

import numpy as np
import xarray as xr
import geopandas as gpd
import pandas as pd

from shapely.affinity import translate
from shapely.geometry import Point

from emaremes.utils import Extent

# Read polygons
subcatchments = gpd.read_file(
    "../../data_old/Subcatchments__WestLittleCal/Subcatchments__WestLittleCal.shp"
)
subcatchments.explore(column="Id", categorical=True)

# Make a single polygon
subcatchments["geometry"] = subcatchments["geometry"].buffer(0.001)
blob = subcatchments.dissolve().simplify(tolerance=50)

# Extract Polygon from GeoSeries and translate to GRIB coordinates
blob = blob.to_crs("4326")
translated_polygon = translate(blob.geometry[0], xoff=360)

# Set a extent to make a clip of the GRIB data before masking
bounds = blob.bounds
extent = Extent((bounds.miny[0], bounds.maxy[0]), (bounds.minx[0], bounds.maxx[0]))

# Select GRIB2 files
data_folder = Path("../../data_old/20210626/")
idx_files = data_folder.glob("*.idx")

for idx in idx_files:
    idx.unlink()

grib_files = data_folder.glob("*.grib2")
grib_files = sorted(grib_files)

# Generate a mask from the first grib file

with xr.open_dataset(grib_files[0], engine="cfgrib", decode_timedelta=True) as ds:
    xclip = ds.loc[extent.as_xr_slice()]

    # Generate points to evaluate
    lon, lat = xclip.longitude, xclip.latitude
    llon, llat = np.meshgrid(lon, lat)
    points = np.vstack((llon.flatten(), llat.flatten())).T

    # Mask using the polygon.contains calculation
    mask = [translated_polygon.contains(Point(x, y)) for x, y in points]
    mask = np.array(mask).reshape(len(lat), len(lon))


def extract_mean(file: Path):
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


with Pool() as pool:
    query = pool.map(extract_mean, grib_files)

df = pd.DataFrame(
    {
        "timestamp": pd.DatetimeIndex([q[0] for q in query]),
        "value": [q[1] for q in query],
    },
)

df["timestamp"] = pd.to_datetime(df["timestamp"])
df["value"] = df["value"].astype(float)
df.set_index("timestamp", inplace=True)
df.to_parquet("multip.parquet")
