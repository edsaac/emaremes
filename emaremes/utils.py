from dataclasses import dataclass


@dataclass
class Extent:
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
        if self.left_lon < 0:
            pos_left_lon = 360 + self.left_lon

        if self.right_lon < 0:
            pos_right_lon = 360 + self.right_lon

        return dict(
            latitude=slice(self.up_lat, self.down_lat),
            longitude=slice(pos_left_lon, pos_right_lon),
        )

    def as_mpl(self):
        return (self.left_lon, self.right_lon, self.down_lat, self.up_lat)
