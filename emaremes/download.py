from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from multiprocessing import Pool
from itertools import compress
from typing import Literal

import requests
import pandas as pd

from .utils import DATA_NAMES

type DatetimeLike = datetime | pd.Timestamp
type MRMSDataType = Literal[
    "precip_rate",
    "precip_flag",
    "precip_accum_1h",
    "precip_accum_24h",
    "precip_accum_72h",
]

_BASE_URL = "https://mtarchive.geol.iastate.edu"
LOCALPATH = Path.home() / "emaremes"

if not LOCALPATH.exists():
    LOCALPATH.mkdir()
    print(f"Downloaded MRMS data will be stored at {LOCALPATH}")


@dataclass
class GribFile:
    """
    Helper class to generate a grib file URL and path.
    """

    t: DatetimeLike
    data_type: MRMSDataType = "precip_rate"

    def __post_init__(self):
        if not isinstance(self.t, pd.Timestamp):
            self.t = pd.to_datetime(self.t)

        match self.data_type:
            case "precip_rate" | "precip_flag":
                self.t = self.t.replace(second=0, microsecond=0)

                if self.t.minute % 2 != 0:
                    raise ValueError(
                        f"{self.t} is invalid. GRIB files are posted every 2 minutes"
                    )

            case "precip_accum_1h" | "precip_accum_24h" | "precip_accum_72h":
                self.t = self.t.replace(minute=0, second=0, microsecond=0)

    @property
    def url(self) -> str:
        tail = DATA_NAMES[self.data_type]
        head = f"{_BASE_URL}/{self.t.strftime(r'%Y/%m/%d')}/mrms/ncep/{tail}"
        return f"{head}/{tail}_00.00_{self.t.strftime(r'%Y%m%d-%H%M%S')}.grib2.gz"

    @property
    def fname(self) -> str:
        return self.url.rpartition("/")[-1]

    @property
    def folder(self) -> Path:
        return LOCALPATH / self.t.strftime(r"%Y%m%d")

    @property
    def path(self) -> Path:
        return self.folder / self.fname

    def exists(self) -> bool:
        return self.path.exists()


def single_file(gfile: GribFile, verbose: bool = False):
    """
    Requests a GribFile from the base URL to the MRMS archive.

    Parameters
    ----------
    gfile : GribFile
        File to be downloaded
    verbose : bool, optional
        Whether to print the progress of the download, by default False.

    Returns
    -------
    None
    """
    r = requests.get(gfile.url, stream=True)

    if r.status_code == 200:
        # Make sure YYYYMMDD folder exists
        gfile.folder.mkdir(exist_ok=True, parents=True)

        # Write data to file
        with open(gfile.path, "wb") as f:
            f.write(r.content)
            if verbose:
                print(f"Saved {gfile.fname} :)")
    else:
        if verbose:
            print(f"Error downloading {gfile.fname}. Likely it does not exist.")


def timerange(
    initial_datetime: DatetimeLike,
    end_datetime: DatetimeLike,
    frequency: timedelta = timedelta(minutes=10),
    data_type: MRMSDataType = "precip_rate",
    verbose: bool = False,
):
    """
    Download MRMS files available in the time range.

    Parameters
    ----------
    initial_datetime : DatetimeLike
        Initial datetime.
    end_datetime : DatetimeLike
        File to be downloaded.
    frequency : timedelta = timedelta(minutes=10)
        Frequency of files to download. Precipitation rate and flags are available every
        2 minutes. 24h accumulated precipitation is available every hour.
    data_type : DataType, optional
        Type of data to download, by default "precip_rate". Other options are
        "precip_flag" and "precip_accum_24h".
    verbose : bool, optional
        Whether to print the progress of the download, by default False.

    Returns
    -------
    list[Path]
        List of paths with the downloaded files.
    """
    if frequency < timedelta(minutes=2):
        raise ValueError("`frequency` should not be less than 2 minutes")

    # Generate range of files
    initial_datetime = initial_datetime.replace(second=0, microsecond=0)
    end_datetime = end_datetime.replace(second=0, microsecond=0)

    range_dates = pd.date_range(initial_datetime, end_datetime, freq=frequency)
    gfiles = [GribFile(t, data_type) for t in range_dates]

    for dest_folder in set([gf.folder for gf in gfiles]):
        dest_folder.mkdir(exist_ok=True)

        dest_folder.glob("*.idx")
        for idx in dest_folder.glob("*.idx"):
            idx.unlink()

    # Select which files need to be downloaded
    mask = [not gf.exists() for gf in gfiles]
    gfiles_missing = list(compress(gfiles, mask))

    if gfiles_missing:
        if verbose:
            print(f"-> {len(gfiles_missing)} files will be requested...")

        with Pool() as pool:
            pool.map(single_file, gfiles_missing)

    else:
        if verbose:
            print("Nothing new to download :D")

    return [gf.path for gf in gfiles]
