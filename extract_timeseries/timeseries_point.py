from pathlib import Path
from multiprocessing import Pool
import argparse

import pandas as pd
import xarray as xr


def extract_point_value(f: Path, lat: float, lon: float):
    ds = xr.open_dataset(f, engine="cfgrib", decode_timedelta=False)
    time = ds.time.values.copy()
    val = ds.sel(latitude=lat, longitude=lon, method="nearest")["unknown"].values.copy()
    ds.close()

    return time, val


def extract_series(files: list[Path], lat: float, lon: float):
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--data", help="Folder with GRIB2 files", type=Path)
    parser.add_argument("--lat", help="Latitude", type=float)
    parser.add_argument("--lon", help="Longitude", type=float)

    args = parser.parse_args()

    data = args.data
    lat = args.lat
    lon = args.lon if args.lon > 0 else 360 + args.lon

    if data.is_dir():
        files = Path(args.input).glob("*.grib2")
        files = sorted(files)

        out_folder = Path(f"./{args.input.name}")
        out_folder.mkdir(exist_ok=True)

        df = extract_series(files, lat, lon)
        df.to_csv(f"{out_folder}/{lat:.2f}-{lon:.2f}.txt")

    else:
        raise ValueError(f"{data} is not a folder")
