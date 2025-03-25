from pathlib import Path
from typing import Literal

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cmocean
import cartopy.crs as ccrs
import cartopy.feature as cf

from .utils import Extent, STATE_BOUNDS
from .typing_utils import US_State


def plot_map(file: Path, state: US_State | Literal["CONUS"]):
    state = state.upper()

    if state == "CONUS":
        extent = Extent((20, 55), (-125, -60))

    elif state not in STATE_BOUNDS:
        raise ValueError(f"State {state} not found.")

    else:
        extent = STATE_BOUNDS[state]

    # Map settings
    proj = ccrs.Orthographic(**extent.as_cartopy_center())
    plate = ccrs.PlateCarree()

    with xr.open_dataset(file, engine="cfgrib", decode_timedelta=False) as ds:
        # Mask out no data (-3 for precipitation data) and hide small intensities
        ds = ds.where(ds["unknown"] != -3).where(ds["unknown"] >= 1)

        # Downscale to ~0.1km resolution
        xclip = ds.loc[extent.as_xr_slice()]
        coarse = xclip.coarsen(latitude=2, longitude=2, boundary="pad").mean()

        # Set boundaries
        fig = plt.figure(figsize=(7, 8))
        ax = fig.add_subplot(1, 1, 1, projection=proj)

        # CONUS extent
        ax.set_extent(extent.as_mpl(), crs=plate)
        ax.add_feature(cf.LAKES, alpha=0.3, zorder=1)
        ax.add_feature(cf.OCEAN, alpha=0.3, zorder=1)
        ax.add_feature(cf.STATES, zorder=1, lw=0.5, ec="gray")
        ax.add_feature(cf.COASTLINE, zorder=1, lw=0.5)

        coarse["unknown"].plot(
            ax=ax,
            vmin=0,
            vmax=50,
            zorder=4,
            alpha=0.9,
            transform=plate,
            cmap=cmocean.cm.rain,
            cbar_kwargs=dict(label="PrecipRate [mm/hr]", shrink=0.35),
        )

        timestr = np.datetime_as_string(coarse.time.values.copy(), unit="s")
        ax.set_title(timestr, fontsize=10)

        for spine in ax.spines:
            ax.spines[spine].set_visible(False)

    return fig
