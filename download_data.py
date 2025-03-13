from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from multiprocessing import Pool
from itertools import compress

import requests
import pandas as pd

BASE_URL = "https://mtarchive.geol.iastate.edu"
MRMS_DATA = Path("./data")


@dataclass
class GribFile:
    t: pd.Timestamp

    @property
    def url(self) -> str:
        head = f"{BASE_URL}/{self.t.strftime(r'%Y/%m/%d')}/mrms/ncep/PrecipRate"
        return f"{head}/PrecipRate_00.00_{self.t.strftime(r'%Y%m%d-%H%M%S')}.grib2.gz"

    @property
    def fname(self) -> str:
        return self.url.rpartition("/")[-1]

    @property
    def folder(self) -> Path:
        return MRMS_DATA / self.t.strftime(r"%Y%m%d")

    @property
    def path(self) -> Path:
        return self.folder / self.fname

    def exists(self) -> bool:
        return self.path.exists()


def get_file(gfile: GribFile):
    r = requests.get(gfile.url, stream=True)

    if r.status_code == 200:
        with open(gfile.path, "wb") as f:
            f.write(r.content)
            print(f"Saved {gfile.fname} :)")
    else:
        print(f"Error downloading {gfile.fname}")


if __name__ == "__main__":
    if not MRMS_DATA.exists():
        MRMS_DATA.mkdir()

    # Insert date range to query and frequency
    initial_date = datetime(2021, 6, 27, 0, 0, 0)
    end_date = datetime(2021, 6, 28, 0, 0, 0)
    frequency = timedelta(minutes=30)

    # Generate range of files
    range_dates = pd.date_range(initial_date, end_date, freq=frequency)
    gfiles = [GribFile(t) for t in range_dates]

    for dest_folder in set([gf.folder for gf in gfiles]):
        dest_folder.mkdir(exist_ok=True)

    # Select which files need to be downloaded
    mask = [not gf.exists() for gf in gfiles]
    gfiles_missing = list(compress(gfiles, mask))

    if gfiles_missing:
        print(f">> {len(gfiles_missing)} files will be requested...")

        with Pool() as pool:
            pool.map(get_file, gfiles_missing)

    else:
        print("Nothing new to download :D")
