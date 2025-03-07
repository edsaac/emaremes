from pathlib import Path
from sys import argv

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cf

OUT_IMG = Path("imgs")


def main(file: Path):
    if file.suffix != ".grib2":
        raise TypeError("File type is not grib2")

    ds = xr.open_dataset(file, engine="cfgrib", decode_timedelta=False)

    # Bounded to IL and masking NaN pixels
    xclip = ds.loc[dict(latitude=slice(43.5, 36), longitude=slice(268, 274))]
    masked = xclip.where(xclip["unknown"] != -3)
    masked = masked.where(xclip["unknown"] != 0)

    # Get timestamp
    timestr = np.datetime_as_string(ds.time.values.copy(), unit="s")

    # Define cartopy projections
    proj = ccrs.LambertConformal(central_longitude=-87.688, central_latitude=41.607)
    plate = ccrs.PlateCarree()

    fig = plt.figure(figsize=(7, 6))
    ax = fig.add_subplot(1, 1, 1, projection=proj)

    # Illinois extent
    ax.set_extent((-92, -86.5, 36.5, 43.5), crs=plate)

    # Add some map features
    ax.add_feature(cf.LAKES, alpha=0.2, zorder=1)
    ax.add_feature(cf.STATES, zorder=1)
    ax.gridlines(draw_labels=False, zorder=2, lw=1, ls="dotted")

    ax.scatter([-87.688], [41.607], transform=plate, marker="x", c="k", s=30, zorder=3)

    masked["unknown"].plot(
        cmap="gist_ncar_r",
        vmin=0,
        vmax=100,
        ax=ax,
        zorder=4,
        transform=plate,
        cbar_kwargs=dict(label="PrecipRate [mm/hr]", shrink=0.35),
    )

    ax.set_title(timestr, fontsize=10)
    fig.savefig(OUT_IMG / f"{file.name}.png", bbox_inches="tight", dpi=120)
    plt.close(fig)


if __name__ == "__main__":
    if len(argv) == 1:
        print("No file was provided")
        file = Path("data/20190430/PrecipRate_00.00_20190430-060000.grib2")

    else:
        file = Path(argv[1])

    main(file)
