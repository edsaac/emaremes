from pathlib import Path
from multiprocessing import Pool
from typing import Literal
from sys import argv

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt
import geopandas as gpd
import cmocean

from emaremes.utils import Extent

FOLDER_DESTINATION = Path("imgs_cook_county")
FOLDER_DESTINATION.mkdir(exist_ok=True)

img = cimgt.OSM(cache=True)


# Read shapefile
municipalities = gpd.read_file("~/Desktop/GIS_drafts/Municipalities/all_municipalities.geojson")
markham = municipalities.loc[municipalities["MUNICIPALITY"].isin(["Markham"])]
chicago = municipalities.loc[municipalities["MUNICIPALITY"].isin(["Chicago"])]
cook_county_border = gpd.read_file("~/Desktop/GIS_drafts/Municipalities/Cook_County_Border.geojson")

extent = Extent((41.40, 42.2), (-88.30, -87.45))
lightcmap = cmocean.tools.lighten(cmocean.cm.rain, 0.9)


def make_figure(file: Path, what_to_do: Literal["return_fig", "save_fig", "show_fig"] = "save_fig"):
    # Bounded to IL and masking NaN pixels
    ds = xr.open_dataset(file, engine="cfgrib", decode_timedelta=False)
    timestr = np.datetime_as_string(ds.time.values.copy(), unit="s")

    xclip = ds.loc[extent.as_xr_slice()]
    masked = xclip.where(xclip["unknown"] != -3)
    masked = masked.where(xclip["unknown"] != 0)

    # Define cartopy projections
    # proj = ccrs.AzimuthalEquidistant(central_longitude=-87.688, central_latitude=41.607)
    proj = ccrs.PlateCarree()
    plate = ccrs.PlateCarree()

    fig = plt.figure(figsize=(7, 6))
    ax = fig.add_subplot(1, 1, 1, projection=proj)

    # Illinois extent
    ax.set_extent(extent.as_mpl(), crs=plate)

    # Add some map features
    ax.add_image(img, 10, zorder=1)
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

    boundaries_kwargs = dict(transform=plate, zorder=5, facecolor="None", edgecolor="#111")
    markham.plot(ax=ax, **boundaries_kwargs)
    chicago.plot(ax=ax, **boundaries_kwargs)
    cook_county_border.plot(ax=ax, **boundaries_kwargs)

    ax.set_title(timestr, fontsize=10)
    for spine in ax.spines:
        ax.spines[spine].set_visible(False)

    if what_to_do == "show_fig":
        plt.show()
        plt.close(fig)
        return

    elif what_to_do == "return_fig":
        return fig

    elif what_to_do == "save_fig":
        plt.savefig(FOLDER_DESTINATION / f"{file.name}.png", bbox_inches="tight", dpi=120)
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
