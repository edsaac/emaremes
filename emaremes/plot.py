from typing import Literal
from pathlib import Path

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cmocean
import cartopy.crs as ccrs
import cartopy.feature as cf

from matplotlib import colors as _colors

from .utils import (
    STATE_BOUNDS,
    PRECIP_FLAGS,
    DATA_NAMES,
    PRECIP_FLAGS_COLORS,
    Extent,
    unzip_if_gz,
)

from .typing_utils import US_State

__all__ = ["precip_rate_map"]


@unzip_if_gz
def precip_rate_map(file: Path, state: US_State | Literal["CONUS"]) -> plt.Figure:
    """
    Make a map of precipitation rate.

    Parameters
    ----------
    file : Path
        Path to a file containing precipitation rate data.
    state : US_State | Literal["CONUS"]
        State to plot. If "CONUS", the entire CONUS is plotted.

    Returns
    -------
    plt.Figure
        Figure object. It uses the `cartopy` library to plot the map.
    """

    if DATA_NAMES["precip_rate"] not in file.name:
        raise ValueError(f"File {file} does not contain precipitation rate data.")

    state = state.upper()

    if state == "CONUS":
        extent = Extent((20, 55), (-125, -60))
        scale_win = 10

    elif state in STATE_BOUNDS:
        extent = STATE_BOUNDS[state]
        scale_win = 2

    else:
        raise ValueError(f"State {state} not found.")

    # Map settings
    proj = ccrs.Orthographic(**extent.as_cartopy_center())
    plate = ccrs.PlateCarree()

    with xr.open_dataset(file, engine="cfgrib", decode_timedelta=False) as ds:
        # Mask out no data (-3 for precipitation data) and hide small intensities
        ds = ds.where(ds["unknown"] != -3).where(ds["unknown"] >= 1)

        # Downscale to ~0.1km resolution
        xclip = ds.loc[extent.as_xr_slice()]
        coarse = xclip.coarsen(latitude=scale_win, longitude=scale_win, boundary="pad").mean()

        # Set boundaries
        fig = plt.figure(figsize=(12, 12))
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


@unzip_if_gz
def precip_flag_map(file: Path, state: US_State | Literal["CONUS"]) -> plt.Figure:
    """
    Make a map of precipitation types.

    Parameters
    ----------
    file : Path
        Path to a file containing precipitation rate data.
    state : US_State | Literal["CONUS"]
        State to plot. If "CONUS", the entire CONUS is plotted.

    Returns
    -------
    plt.Figure
        Figure object. It uses the `cartopy` library to plot the map.
    """

    if DATA_NAMES["precip_flag"] not in file.name:
        raise ValueError(f"File {file} does not contain precipitation flag data.")

    state = state.upper()

    if state == "CONUS":
        extent = Extent((20, 55), (-125, -60))
        scale_win = 10

    elif state in STATE_BOUNDS:
        extent = STATE_BOUNDS[state]
        scale_win = 2

    else:
        raise ValueError(f"State {state} not found.")

    # Map settings
    proj = ccrs.Orthographic(**extent.as_cartopy_center())
    plate = ccrs.PlateCarree()

    # Colorbar settings
    cmap = _colors.ListedColormap(list(PRECIP_FLAGS_COLORS.values()))
    bounds = [-0.5, 0.5, 2, 4.5, 6.5, 8.5, 50.5, 93.5, 100]
    ticks = [(i + j) / 2 for i, j in zip(bounds[:-1], bounds[1:])]
    norm = _colors.BoundaryNorm(bounds, cmap.N)

    with xr.open_dataset(file, engine="cfgrib", decode_timedelta=False) as ds:
        # Mask out no data (-3 for precipitation data) and hide no rain
        ds = ds.where(ds["unknown"] != -3).where(ds["unknown"] != 0)

        # Downscale to ~0.1km resolution
        xclip = ds.loc[extent.as_xr_slice()]
        coarse = xclip.coarsen(latitude=scale_win, longitude=scale_win, boundary="pad").mean()

        # Set boundaries
        fig = plt.figure(figsize=(12, 12))
        ax = fig.add_subplot(1, 1, 1, projection=proj)

        # CONUS extent
        ax.set_extent(extent.as_mpl(), crs=plate)
        ax.add_feature(cf.LAKES, alpha=0.3, zorder=1)
        ax.add_feature(cf.OCEAN, alpha=0.3, zorder=1)
        ax.add_feature(cf.STATES, zorder=1, lw=0.5, ec="gray")
        ax.add_feature(cf.COASTLINE, zorder=1, lw=0.5)

        img = coarse["unknown"].plot(
            ax=ax, zorder=4, alpha=0.9, transform=plate, cmap=cmap, norm=norm, add_colorbar=False
        )

        cb = plt.colorbar(img, ax=ax, label=None, shrink=0.35)
        cb.ax.yaxis.set_ticks_position("none")
        cb.ax.yaxis.set_ticks(ticks)
        cb.ax.set_yticklabels(list(PRECIP_FLAGS.values()), fontdict={"fontsize": 8, "weight": 300})

        timestr = np.datetime_as_string(coarse.time.values.copy(), unit="s")
        ax.set_title(timestr, fontsize=10)

        for spine in ax.spines:
            ax.spines[spine].set_visible(False)

    return fig
