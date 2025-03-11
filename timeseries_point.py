from pathlib import Path
from multiprocessing import Pool

import pandas as pd
import xarray as xr


def extract_point_value(f: Path, lat: float, lon: float):
    ds = xr.open_dataset(f, engine="cfgrib", decode_timedelta=False)

    time = ds.time.values.copy()
    val = ds.sel(latitude=lat, longitude=360 + lon, method="nearest")["unknown"].values.copy()

    ds.close()

    return time, val


def extract_series(date_folder: str, lat: float, lon: float):
    files = Path(f"data/{date_folder}").glob("*.grib2")
    files = sorted(files)

    with Pool() as pool:
        query = pool.starmap(extract_point_value, [(f, lat, lon) for f in files])

    df = pd.DataFrame(
        {
            "timestamp": pd.DatetimeIndex([q[0] for q in query]),
            "value": [q[1] for q in query],
        }
    )
    df.set_index("timestamp", inplace=True)
    df.to_csv("timeseries/data.txt")

    return df


if __name__ == "__main__":
    pass
