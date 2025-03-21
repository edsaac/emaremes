from dataclasses import dataclass


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
