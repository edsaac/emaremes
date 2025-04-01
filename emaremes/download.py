from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from multiprocessing import Pool
from itertools import compress, product

import requests
import pandas as pd

from .utils import DATA_NAMES, _PathConfig
from .typing_utils import MRMSDataType

type DatetimeLike = datetime | pd.Timestamp


_BASE_URL: str = "https://mtarchive.geol.iastate.edu"


path_config = _PathConfig()


@dataclass
class GribFile:
    """
    Helper class to generate a grib file URL and a path.
    """

    t: DatetimeLike
    data_type: MRMSDataType = "precip_rate"

    def __post_init__(self):
        # Make sure t is a pd.Timestamp
        if not isinstance(self.t, pd.Timestamp):
            self.t = pd.to_datetime(self.t)

        # Check if the data_type is valid
        match self.data_type:
            case "precip_rate" | "precip_flag":
                self.t = self.t.replace(second=0, microsecond=0)

                if self.t.minute % 2 != 0:
                    raise ValueError(f"{self.t} is invalid. GRIB files are posted every 2 minutes")

            case "precip_accum_1h" | "precip_accum_24h" | "precip_accum_72h":
                self.t = self.t.replace(minute=0, second=0, microsecond=0)

        # Check if the file exists in anywhere in path_config.
        # If it does, set that as the GribFile path. Otherwise,
        # set the path to the file in the last ADDPATHS folder.

        subdir: str = self.t.strftime(r"%Y%m%d")
        gz_name: str = self.url.rpartition("/")[-1]
        grib_name: str = gz_name.rpartition(".")[0]

        for root, name in product(reversed(path_config.all_paths), (grib_name, gz_name)):
            if (root / subdir / name).exists():
                self.root = root
                self._path = root / subdir / name
                break
        else:
            self.root = path_config.prefered_path
            self._path = self.root / subdir / gz_name

        self.subdir = self.root / subdir

    @property
    def url(self) -> str:
        """Assemble the URL to the MRMS archive"""
        tail = DATA_NAMES[self.data_type]
        head = f"{_BASE_URL}/{self.t.strftime(r'%Y/%m/%d')}/mrms/ncep/{tail}"
        return f"{head}/{tail}_00.00_{self.t.strftime(r'%Y%m%d-%H%M%S')}.grib2.gz"

    @property
    def path(self) -> Path:
        return self._path

    @property
    def filename(self) -> str:
        return self._path.name

    def exists(self) -> bool:
        return self._path.exists()


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
    if gfile.exists():
        if verbose:
            print(f"{gfile._path} already exists. Skipping.")
        return

    r = requests.get(gfile.url, stream=True)

    if r.status_code == 200:
        # Make sure YYYYMMDD folder exists
        gfile.subdir.mkdir(exist_ok=True, parents=True)

        # Write data to file
        with open(gfile._path, "wb") as f:
            f.write(r.content)
            if verbose:
                print(f"Saved {gfile._path} :)")
    else:
        if verbose:
            print(f"Error downloading {gfile.filename}. Likely it does not exist.")


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

    for dest_folder in set([gf.subdir for gf in gfiles]):
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

    return [gf._path for gf in gfiles]
