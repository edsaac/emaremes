import pickle
from pathlib import Path
from collections import namedtuple
from itertools import chain
import pandas as pd
from timeseries_point import extract_series

GlobHelper = namedtuple("GlobHelper", ["date", "hour"])
NamedCoord = namedtuple("NamedCoord", ["name", "lat", "lon"])

usgs_data_path = Path("usgs/usgs_Cook County.pkl")

with open(usgs_data_path, "rb") as f:
    data = pickle.load(f)
    daily, inst, site_info, pcodes = data.values()

print(f"{len(site_info)} lat/lon pairs will be queried")

MONTH = "08"
START_DATE = pd.Timestamp(f"2021{MONTH}01", tz="utc")
END_DATE = pd.Timestamp(f"2021{MONTH}31", tz="utc")

for i, site in site_info.iterrows():
    """"""
    ## Get site coordinates and number
    site_no = site["site_no"]
    coord = NamedCoord(
        site["station_nm"],
        site["dec_lat_va"],
        360 + site["dec_long_va"],
    )
    dest_folder = Path(f"./timeseries/{MONTH}")
    dest_folder.mkdir(parents=True, exist_ok=True)
    parquet_file = dest_folder / f"{coord.name}.parquet"

    if parquet_file.exists():
        print(f"{parquet_file.name} already exists")
        continue

    ## Filter hours with rain - skip zeros from query
    files = []
    resampled = inst.xs(site_no).loc[START_DATE:END_DATE].resample("1h").sum()
    filtered = resampled[resampled["00045"] > 0]
    hours_with_storms = [GlobHelper(t.strftime("%Y%m%d"), t.strftime("%H")) for t in filtered.index]

    for hd in hours_with_storms:
        pattern = f"{hd.date}/*{hd.date}-{hd.hour}*.gz"
        ls = Path("../data").glob(pattern)
        ls = sorted(ls)
        files.append(ls)

    files = list(chain(*files))

    ## Execute GRIB2 query
    df = extract_series(files, coord.lat, coord.lon)
    df.to_parquet(parquet_file)
