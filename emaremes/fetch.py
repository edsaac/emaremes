from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import compress, product
from multiprocessing import Pool
from pathlib import Path
from typing import get_args

import requests
import pandas as pd

from .utils import DATA_NAMES, _PathConfig
from .typing_utils import MRMSDataType

type DatetimeLike = str | date | datetime | pd.Timestamp
type TimedeltaLike = str | timedelta | pd.Timedelta

_BASE_URL: str = "https://mtarchive.geol.iastate.edu"

__all__ = ["path_config", "timerange"]

path_config = _PathConfig()


@dataclass
class _GribFile:
    """
    Helper class to generate a grib file URL and a path.
    """

    t: DatetimeLike
    data_type: MRMSDataType = "precip_rate"

    def __post_init__(self):
        if not isinstance(self.t, pd.Timestamp):
            self.t: pd.Timestamp = pd.to_datetime(self.t)

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
        # set the path to the file in the prefered folder.

        subdir: str = self.t.strftime(r"%Y%m%d")
        gz_name: str = self.url.rpartition("/")[-1]
        grib_name: str = gz_name.rpartition(".")[0]

        ordered_paths = [path_config.prefered_path]
        if other_paths := path_config.all_paths - {path_config.prefered_path}:
            ordered_paths += list(other_paths)

        for root, name in product(ordered_paths, (grib_name, gz_name)):
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
        mrms_datatype = DATA_NAMES[self.data_type]
        header = f"{_BASE_URL}/{self.t.strftime(r'%Y/%m/%d')}/mrms/ncep/{mrms_datatype}"
        return f"{header}/{mrms_datatype}_00.00_{self.t.strftime(r'%Y%m%d-%H%M%S')}.grib2.gz"

    @property
    def path(self) -> Path:
        return self._path

    @property
    def filename(self) -> str:
        return self._path.name

    def exists(self) -> bool:
        return self._path.exists()


def _single_file(gfile: _GribFile, verbose: bool = False):
    """
    Requests a GribFile from the base URL and stores it into the MRMS default path.

    Parameters
    ----------
    gfile : GribFile
        File to be requested.
    verbose : bool, optional
        Whether to print the progress of the download, by default False.
    """

    if not isinstance(gfile, _GribFile):
        raise ValueError("`gfile` must be a _GribFile instance")

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
    frequency: TimedeltaLike = pd.Timedelta(minutes=10),
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
    frequency : TimedeltaLike = pd.Timedelta(minutes=10)
        Frequency of files to download. Precipitation rate and flags are available every
        2 minutes. 24h accumulated precipitation is available every hour.
    data_type : MRMSDataType, optional
        Type of data to download, by default "precip_rate". Other options are
        "precip_flag" and "precip_accum_24h".
    verbose : bool, optional
        Whether to print the progress of the download, by default False.

    Returns
    -------
    list[Path]
        List of paths with the downloaded files.
    """
    if not isinstance(initial_datetime, pd.Timestamp):
        initial_datetime = pd.Timestamp(initial_datetime)

    if not isinstance(end_datetime, pd.Timestamp):
        end_datetime = pd.Timestamp(end_datetime)

    if initial_datetime > end_datetime:
        raise ValueError("`initial_datetime` must come before `end_datetime`")

    if not isinstance(frequency, pd.Timedelta):
        frequency = pd.Timedelta(frequency)

    if frequency < pd.Timedelta(minutes=2):
        raise ValueError("`frequency` should not be less than 2 minutes")

    if data_type not in get_args(MRMSDataType.__value__):
        raise KeyError(f"`data_type` must be one of: {get_args(MRMSDataType.__value__)}")

    # Generate range of files
    initial_datetime = initial_datetime.replace(second=0, microsecond=0)
    end_datetime = end_datetime.replace(second=0, microsecond=0)

    range_dates = pd.date_range(initial_datetime, end_datetime, freq=frequency)
    print(f"-> {len(range_dates)} files were requested...")

    gfiles = [_GribFile(t, data_type) for t in range_dates]

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
            print(f"-> {len(gfiles_missing)} *new* files will be downloaded...")

        with Pool() as pool:
            pool.starmap(_single_file, [(f, verbose) for f in gfiles_missing])

    else:
        if verbose:
            print("Nothing new to download :D")

    return [gf._path for gf in gfiles]
