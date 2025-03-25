"""
Benchmark:
717 GRIB files (1 day @ 2min)

naive -> 5m28.752s
multip -> 0m55.990s
multip.2 -> 0m56.370s
"""

from pathlib import Path
import geopandas as gpd

from emaremes.ts import extract_polygon_series


# Read polygons
subcatchments = gpd.read_file(
    "../../data_old/Subcatchments__WestLittleCal/Subcatchments__WestLittleCal.shp"
)

# Make a single polygon
subcatchments["geometry"] = subcatchments["geometry"].buffer(0.001)
blob = subcatchments.dissolve().simplify(tolerance=50)

# Select GRIB2 files
data_folder = Path("../../data_old/20210626/")
idx_files = data_folder.glob("*.idx")

for idx in idx_files:
    idx.unlink()

grib_files = data_folder.glob("*.grib2")
grib_files = sorted(grib_files)

df = extract_polygon_series(grib_files, blob, upsample=True)
df.to_parquet("multip_intepr.parquet")
