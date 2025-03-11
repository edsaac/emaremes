from datetime import datetime, timedelta
from pathlib import Path
from multiprocessing import Pool

import requests
import pandas as pd

BASE_URL = "https://mtarchive.geol.iastate.edu"
MRMS_DATA = Path("data")


def get_file(t: pd.Timestamp):
    # Create folder to store data
    STORE = MRMS_DATA / Path(t.strftime(r"%Y%m%d"))
    STORE.mkdir(exist_ok=True)

    url = f"{BASE_URL}/{t.strftime(r'%Y/%m/%d')}/mrms/ncep/PrecipRate"
    url = f"{url}/PrecipRate_00.00_{t.strftime(r'%Y%m%d-%H%M%S')}.grib2.gz"
    filename = url.rpartition("/")[-1]

    if Path(STORE / filename).exists():
        print(f"{filename} already exists.")
        return

    r = requests.get(url, stream=True)

    if r.status_code == 200:
        with open(STORE / filename, "wb") as f:
            f.write(r.content)
            print(f"Saved {filename} :)")
    else:
        print(f"Error downloading {filename}")


def main():
    # Insert date range to query
    initial_date = datetime(2021, 6, 26, 0, 0, 0)
    end_date = datetime(2021, 6, 27, 0, 0, 0)

    # Select a frequency.
    # data is posted every 2 minutes
    frequency = timedelta(minutes=2)

    range_dates = pd.date_range(initial_date, end_date, freq=frequency)

    with Pool() as pool:
        pool.map(get_file, range_dates)


if __name__ == "__main__":
    if not MRMS_DATA.exists():
        MRMS_DATA.mkdir()

    main()
