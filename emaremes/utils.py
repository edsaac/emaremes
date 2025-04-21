import gzip
import functools
from os import PathLike

from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from pathlib import Path
from typing import Callable, Concatenate

__all__ = [
    "Extent",
    "DATA_NAMES",
    "PRECIP_FLAGS",
    "PRECIP_FLAGS_COLORS",
    "STATE_BOUNDS",
    "unzip_if_gz",
]


@dataclass
class Extent:
    """
    Helper class to represent a geographical extent.

    Parameters
    ----------
    lats : tuple[float, float]
        Latitude range of the extent.
    lons : tuple[float, float]
        Longitude range of the extent.
    """

    lats: tuple[float, float]
    lons: tuple[float, float]

    def __post_init__(self):
        if self.lats[0] > self.lats[1]:
            self.up_lat, self.down_lat = self.lats
        else:
            self.down_lat, self.up_lat = self.lats

        if self.lons[0] < self.lons[1]:
            self.left_lon, self.right_lon = self.lons
        else:
            self.right_lon, self.left_lon = self.lons

    @property
    def center(self):
        """
        Returns
        -------
        tuple[float, float]
            The center of the extent. The first value is the longitude and the second
            value is the latitude.
        """
        return (self.left_lon + self.right_lon) / 2, (self.down_lat + self.up_lat) / 2

    def as_cartopy_center(self):
        """
        Returns
        -------
        dict[str, float]
            The center of the extent. The first value is the longitude and the second
            value is the latitude.
        """
        return {"central_longitude": self.center[0], "central_latitude": self.center[1]}

    def as_xr_slice(self):
        """
        Longitudes are positive in GRIB files, but they are negative in the geographical
        coordinate system (EPSG:4326). This function converts the longitudes to positive
        values and returns a dict of slices to pass to xarray.

        Returns
        -------
        dict[str, slice]
            Dictionary of slices to pass to xarray.
        """

        # Longitudes are positive in GRIB files, but they are negative
        # in the geographical coordinate system (EPSG:4326).
        pos_left_lon = 360 + self.left_lon if self.left_lon < 0 else self.left_lon
        pos_right_lon = 360 + self.right_lon if self.right_lon < 0 else self.right_lon

        # Add a little buffer
        buffer = 0.01
        buf_right_lon = round(pos_right_lon, 2) + buffer
        buf_left_lon = round(pos_left_lon, 2) - buffer
        buf_top_lat = round(self.up_lat, 2) + buffer
        buf_down_lat = round(self.down_lat, 2) - buffer

        return dict(
            latitude=slice(buf_top_lat, buf_down_lat),
            longitude=slice(buf_left_lon, buf_right_lon),
        )

    def as_mpl(self):
        """
        Maptlotlib needs the extent in the form (left, right, bottom, top).

        Returns
        -------
        tuple[float, float, float, float]
            Extent in the form (left, right, bottom, top).
        """
        return (self.left_lon, self.right_lon, self.down_lat, self.up_lat)

    def as_shapely(self):
        """
        Shapely uses the extent in the form (xmin, ymin, xmax, ymax).

        Returns
        -------
        tuple[float, float, float, float]
            Extent in the form (left, bottom, right, top).
        """
        return (self.left_lon, self.down_lat, self.right_lon, self.up_lat)


DATA_NAMES: dict[str, str] = {
    "precip_rate": "PrecipRate",
    "precip_flag": "PrecipFlag",
    "precip_accum_1h": "RadarOnly_QPE_01H",
    "precip_accum_24h": "RadarOnly_QPE_24H",
    "precip_accum_72h": "RadarOnly_QPE_72H",
}

PRECIP_FLAGS: dict[int, str] = {
    0: "No precipitation",
    1: "Warm stratiform rain",
    3: "Snow",
    6: "Convective rain",
    7: "Rain mixed with hail",
    10: "Cold stratiform rain",
    91: "Tropical/stratiform rain mix",
    96: "Tropical/convective rain mix",
}

PRECIP_FLAGS_COLORS: dict[int, tuple[str, float]] = {
    0: ("#FFFFFF", 0.0),  # No precipitation (transparent)
    1: ("#FF4500", 1.0),  # Warm stratiform rain
    3: ("#666666", 1.0),  # Snow
    6: ("#228B22", 1.0),  # Convective rain
    7: ("#FF8C00", 1.0),  # Rain mixed with hail
    10: ("#4682B4", 1.0),  # Cold stratiform rain
    91: ("#B22222", 1.0),  # Tropical/stratiform rain mix
    96: ("#4B0082", 1.0),  # Tropical/convective rain mix
}

STATE_BOUNDS: dict[str, Extent] = {
    "AL": Extent((30.13, 35.11), (-88.57, -84.79)),
    "AK": Extent((51.11, 71.64), (-179.25, -66.83)),
    "AZ": Extent((31.23, 37.10), (-114.92, -108.05)),
    "AR": Extent((32.90, 36.60), (-94.53, -88.95)),
    "CA": Extent((32.43, 42.11), (-124.51, -114.03)),
    "CO": Extent((36.89, 41.10), (-109.15, -101.94)),
    "CT": Extent((40.89, 42.15), (-73.83, -70.99)),
    "DE": Extent((38.35, 39.94), (-75.89, -74.95)),
    "FL": Extent((24.30, 31.10), (-87.73, -79.93)),
    "GA": Extent((30.26, 35.08), (-85.71, -80.10)),
    "HI": Extent((18.45, 28.56), (-156.10, -154.71)),
    "ID": Extent((41.89, 49.45), (-117.35, -110.04)),
    "IL": Extent((36.87, 42.61), (-91.61, -86.92)),
    "IN": Extent((37.67, 41.86), (-88.20, -85.08)),
    "IA": Extent((40.28, 43.61), (-96.56, -89.94)),
    "KS": Extent((36.89, 40.10), (-102.15, -94.72)),
    "KY": Extent((36.48, 39.25), (-89.67, -81.86)),
    "LA": Extent((28.82, 33.12), (-94.14, -89.88)),
    "ME": Extent((42.97, 47.56), (-71.18, -66.83)),
    "MD": Extent((37.83, 39.83), (-79.58, -74.95)),
    "MA": Extent((41.59, 42.96), (-73.61, -69.83)),
    "MI": Extent((41.60, 48.41), (-90.52, -82.51)),
    "MN": Extent((43.40, 49.48), (-97.33, -89.59)),
    "MS": Extent((30.12, 35.09), (-91.77, -88.19)),
    "MO": Extent((36.48, 40.71), (-95.87, -89.20)),
    "MT": Extent((44.26, 49.10), (-116.15, -103.94)),
    "NE": Extent((39.90, 43.10), (-104.15, -98.40)),
    "NV": Extent((34.90, 42.10), (-120.10, -113.90)),
    "NH": Extent((42.59, 45.40), (-72.67, -70.40)),
    "NJ": Extent((38.82, 41.46), (-75.66, -73.79)),
    "NM": Extent((31.23, 37.10), (-109.14, -102.90)),
    "NY": Extent((40.38, 45.11), (-79.86, -71.75)),
    "NC": Extent((33.74, 36.69), (-84.42, -75.56)),
    "ND": Extent((46.36, 49.10), (-104.15, -96.69)),
    "OH": Extent((38.30, 42.08), (-84.92, -80.62)),
    "OK": Extent((33.54, 37.10), (-103.10, -94.53)),
    "OR": Extent((41.89, 46.39), (-124.66, -116.56)),
    "PA": Extent((39.62, 42.37), (-80.62, -74.79)),
    "RI": Extent((41.04, 42.02), (-71.97, -71.21)),
    "SC": Extent((32.18, 35.32), (-83.45, -79.93)),
    "SD": Extent((43.29, 46.04), (-104.15, -96.56)),
    "TN": Extent((34.88, 36.68), (-90.41, -81.75)),
    "TX": Extent((25.74, 36.60), (-106.75, -93.41)),
    "UT": Extent((36.89, 42.10), (-114.15, -109.15)),
    "VT": Extent((42.63, 45.11), (-73.54, -71.56)),
    "VA": Extent((36.44, 39.56), (-83.78, -75.13)),
    "WA": Extent((45.44, 49.48), (-124.95, -116.56)),
    "WV": Extent((37.09, 40.74), (-80.62, -77.22)),
    "WI": Extent((42.40, 47.18), (-92.99, -86.85)),
    "WY": Extent((40.90, 45.09), (-111.15, -104.15)),
}


def remove_idx_files(f: Path) -> None:
    """
    Removes the index files of a grib2 file.

    Parameters
    ----------
    f : Path
        Path to a grib2 file or a folder containing grib2 files.
    """

    if not f.is_dir():
        f = f.parent

    idx_files = f.glob("*.idx")

    for idx_file in idx_files:
        idx_file.unlink()


def unzip_if_gz[**P, R](func: Callable[Concatenate[PathLike, P], R]) -> Callable[Concatenate[Path, P], R]:
    @functools.wraps(func)
    def wrapped(f: PathLike, *args: P.args, **kwargs: P.kwargs):
        # Convert to Path object
        f = Path(f)

        if f.suffix == ".grib2":
            return func(f, *args, **kwargs)

        elif f.suffix == ".gz":
            prefix = f.stem.partition("_00.00_")[0]
            assert prefix in DATA_NAMES.values(), "Invalid prefix"

            with gzip.open(f, "rb") as gzip_file_in:
                with NamedTemporaryFile("ab+", prefix=f"{prefix}_", suffix=".grib2") as tf:
                    unzipped_bytes = gzip_file_in.read()
                    tf.write(unzipped_bytes)
                    return func(Path(tf.name), *args, **kwargs)

        raise ValueError("File is not `.gz` nor `.grib2`")

    return wrapped


class _PathConfig:
    def __init__(self) -> None:
        self._defaultpath: Path = Path.home() / "emaremes"
        self._allpaths: set[Path] = {self._defaultpath}
        self._preferedpath: Path = self._defaultpath

        if not self._defaultpath.exists():
            self._defaultpath.mkdir()
            print(f"Created `{self._defaultpath}` to store MRMS data.")

    def add_path(self, path: PathLike, *, make_prefered: bool = False) -> None:
        """Append a location to store Gribfiles.

        Parameters
        ----------
        path : PathLike
            Path to set as prefered.
        """

        path = Path(path)

        if path.exists():
            if not path.is_dir():
                raise ValueError(f"{path} exists but is not a directory.")

        else:
            path.mkdir(exist_ok=True, parents=True)

        self._allpaths.add(Path(path))

        if make_prefered:
            self.set_prefered(path)

    def set_prefered(self, path: PathLike) -> None:
        """Set the prefered path to store Gribfiles.

        Parameters
        ----------
        path : PathLike
            Path to set as prefered.
        """

        path = Path(path)

        if path not in self.all_paths:
            self.add_path(path, make_prefered=False)

        self._preferedpath = path
        print("Prefered path to store *new* Gribfiles is ", self.prefered_path)

    @property
    def default_path(self) -> Path:
        return self._defaultpath

    @property
    def all_paths(self) -> set[Path]:
        return self._allpaths

    @property
    def prefered_path(self) -> Path:
        return self._preferedpath

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return f"defaultpath: {self.default_path}\nprefered: {self.prefered_path}"
