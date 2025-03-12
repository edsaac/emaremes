from pathlib import Path
from sys import argv
from multiprocessing import Pool

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cf
import cmocean

from utils import Extent


FOLDER_DESTINATION = Path("imgs_conus")
FOLDER_DESTINATION.mkdir(exist_ok=True)

# Set boundaries
extent = Extent((20, 55), (-130, -60))


def make_figure(file: Path):
    fout = FOLDER_DESTINATION / f"{file.name}.png"

    if fout.exists():
        print(f"{fout} already exists.")
        return

    ds = xr.open_dataset(file, engine="cfgrib", decode_timedelta=False)
    ds = ds.where(ds["unknown"] != -3)  # No Data
    ds = ds.where(ds["unknown"] >= 1)  # No Data
    coarse = ds.coarsen(latitude=10, longitude=10).mean()

    proj = ccrs.Orthographic(central_longitude=-90, central_latitude=40)
    plate = ccrs.PlateCarree()

    fig = plt.figure(figsize=(7, 7))
    ax = fig.add_subplot(1, 1, 1, projection=proj)

    # CONUS extent
    ax.set_extent(extent.as_mpl(), crs=plate)
    ax.add_feature(cf.LAKES, alpha=0.3, zorder=1)
    ax.add_feature(cf.OCEAN, alpha=0.3, zorder=1)
    ax.add_feature(cf.STATES, zorder=1, lw=0.5, ec="gray")
    ax.add_feature(cf.COASTLINE, zorder=1, lw=0.5)

    coarse["unknown"].plot(
        cmap=cmocean.cm.rain,
        vmin=0,
        vmax=40,
        ax=ax,
        zorder=4,
        transform=plate,
        alpha=0.9,
        cbar_kwargs=dict(label="PrecipRate [mm/hr]", shrink=0.35),
    )

    timestr = np.datetime_as_string(coarse.time.values.copy(), unit="s")
    ax.set_title(timestr, fontsize=10)

    for spine in ax.spines:
        ax.spines[spine].set_visible(False)

    plt.savefig(fout, bbox_inches="tight", dpi=120)
    print(f"Saved {file.name}.png")
    plt.close(fig)
    ds.close()

    return


if __name__ == "__main__":
    path = Path(argv[1])

    if path.is_dir():
        files = path.glob("*.grib2")
        files = sorted(files)

        with Pool() as pool:
            pool.map(make_figure, files)

    else:
        make_figure(path)
