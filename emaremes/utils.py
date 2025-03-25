from dataclasses import dataclass
from pathlib import Path


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
        if self.left_lon < 0:
            pos_left_lon = 360 + self.left_lon

        if self.right_lon < 0:
            pos_right_lon = 360 + self.right_lon

        return dict(
            latitude=slice(self.up_lat, self.down_lat),
            longitude=slice(pos_left_lon, pos_right_lon),
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


def remove_idx_files(f: Path):
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
