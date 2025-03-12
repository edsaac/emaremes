from pathlib import Path
from multiprocessing import Pool
from sys import argv

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt
import geopandas as gpd
import cmocean

from utils import Extent


FOLDER_DESTINATION = Path("imgs_markham")
FOLDER_DESTINATION.mkdir(exist_ok=True)


img = cimgt.OSM(cache=True)
lightcmap = cmocean.tools.lighten(cmocean.cm.rain, 0.9)


# Read shapefiles
markham = gpd.read_file("~/Desktop/GIS_drafts/Municipalities/municipalities.geojson")
streets = gpd.read_file("~/Desktop/GIS_drafts/Streets/major_roads.geojson")
extent = Extent((41.58, 41.63), (-87.72, -87.64))


def make_figure(file: Path):
    # Bounded to IL and masking NaN pixels
    ds = xr.open_dataset(file, engine="cfgrib", decode_timedelta=False)
    timestr = np.datetime_as_string(ds.time.values.copy(), unit="s")

    xclip = ds.loc[extent.as_xr_slice()]
    masked = xclip.where(xclip["unknown"] != -3)
    masked = masked.where(xclip["unknown"] != 0)

    # Define cartopy projections
    proj = ccrs.PlateCarree()
    plate = ccrs.PlateCarree()

    fig = plt.figure(figsize=(7, 6))
    ax = fig.add_subplot(1, 1, 1, projection=proj)

    # Illinois extent
    ax.set_extent(extent.as_mpl(), crs=plate)

    # Add some map features
    ax.add_image(img, 13, zorder=1)
    # ax.add_feature(cf.LAKES, alpha=0.2, zorder=1)
    # ax.add_feature(cf.STATES, zorder=1)

    masked["unknown"].plot(
        cmap=lightcmap,
        vmin=0,
        vmax=40,
        ax=ax,
        zorder=4,
        transform=plate,
        cbar_kwargs=dict(label="PrecipRate [mm/hr]", shrink=0.35),
    )

    markham.plot(ax=ax, transform=plate, zorder=5, facecolor="None", edgecolor="green", ls="dashed")
    streets.plot(ax=ax, transform=plate, zorder=5, facecolor="#444", edgecolor="#444")
    ax.scatter([-87.688], [41.607], transform=plate, marker="x", c="k", s=30, zorder=5)

    ax.set_title(timestr, fontsize=10)
    for spine in ax.spines:
        ax.spines[spine].set_visible(False)

    plt.savefig(FOLDER_DESTINATION / f"{file.name}.png", bbox_inches="tight", dpi=120)
    print(f"Saved {file.name}.png")
    plt.close(fig)
    ds.close()


if __name__ == "__main__":
    path = Path(argv[1])

    if path.is_dir():
        files = path.glob("*.grib2")
        files = sorted(files)

        with Pool() as pool:
            pool.map(make_figure, files)

    else:
        make_figure(path)
