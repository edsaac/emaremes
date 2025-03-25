""" """

from pathlib import Path
from sys import argv

import geopandas as gpd
from emaremes.ts import extract_multipolygon_series

MONTH = argv[1]

# Read polygons
subcatchments = gpd.read_file(
    "../data/Subcatchments__WestLittleCal/Subcatchments__WestLittleCal.shp"
)

# Select GRIB2 files
data_folder = Path("../data")
idx_files = data_folder.glob(f"2024{MONTH}*/*.idx")

for idx in idx_files:
    idx.unlink()

grib_files = data_folder.glob(f"2024{MONTH}*/*.grib2")
grib_files = sorted(grib_files)

df = extract_multipolygon_series(grib_files, subcatchments, identifier=None, upsample=True)
df.set_index("timestamp", inplace=True)
df.to_parquet(f"SubLilCal_{MONTH}.parquet")
