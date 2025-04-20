from typing import Literal
from os import PathLike
from pathlib import Path

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cmocean
import cartopy.crs as ccrs
import cartopy.feature as cf

from scipy import stats
from matplotlib import colors as _colors

from .utils import (
    STATE_BOUNDS,
    PRECIP_FLAGS,
    DATA_NAMES,
    PRECIP_FLAGS_COLORS,
    Extent,
    unzip_if_gz,
)

from .typing_utils import UnitedState, MRMSDataType

__all__ = ["precip_rate_map", "precip_flag_map"]


def _make_fig(dar: xr.DataArray, extent: Extent, data_type: MRMSDataType) -> plt.Figure:
    """
    Helper function to make a figure.

    Parameters
    ----------
    dar : xr.DataArray
        DataArray to plot.
    extent : Extent
        Extent of the map.
    data_type : MRMSDataType
        Type of data to plot.

    Returns
    -------
    plt.Figure
        Figure object. It uses the `cartopy` library to plot the map.
    """
    # Map settings
    target_proj = ccrs.Orthographic(**extent.as_cartopy_center())
    plate_carree = ccrs.PlateCarree()

    # Set boundaries
    fig = plt.figure(figsize=(12, 12))
    ax = fig.add_subplot(1, 1, 1, projection=target_proj)

    # CONUS extent
    ax.set_extent(extent.as_mpl(), crs=plate_carree)
    ax.add_feature(cf.LAKES, alpha=0.3, zorder=1)
    ax.add_feature(cf.OCEAN, alpha=0.3, zorder=1)
    ax.add_feature(cf.STATES, zorder=1, lw=0.5, ec="gray")
    ax.add_feature(cf.COASTLINE, zorder=1, lw=0.5)

    match data_type:
        case "precip_rate":
            img = dar.plot(
                ax=ax,
                transform=plate_carree,
                vmin=0,
                vmax=50,
                zorder=4,
                cmap=cmocean.cm.rain,
                cbar_kwargs=dict(label="PrecipRate [mm/hr]", shrink=0.35),
            )

        case "precip_flag":
            # Colorbar settings
            precip_flags_cmap = _colors.ListedColormap(list(PRECIP_FLAGS_COLORS.values()))
            flags_bounds = [-0.5, 0.5, 2, 4.5, 6.5, 8.5, 50.5, 93.5, 100]
            flags_ticks = [(i + j) / 2 for i, j in zip(flags_bounds[:-1], flags_bounds[1:])]
            precip_flags_norm = _colors.BoundaryNorm(flags_bounds, precip_flags_cmap.N)

            img = dar.plot(
                ax=ax,
                transform=plate_carree,
                zorder=4,
                cmap=precip_flags_cmap,
                norm=precip_flags_norm,
                add_colorbar=False,
            )

            cb = plt.colorbar(img, ax=ax, label=None, shrink=0.35)
            cb.ax.yaxis.set_ticks_position("none")
            cb.ax.yaxis.set_ticks(flags_ticks or cb.ax.yaxis.get_major_ticks())
            cb.ax.set_yticklabels(list(PRECIP_FLAGS.values()), fontdict={"fontsize": 8, "weight": 300})

        case "_":
            raise ValueError(f"Data type {data_type} not implemented.")

    timestr = np.datetime_as_string(dar.time.values.copy(), unit="s")
    ax.set_title(timestr, fontsize=10)

    for spine in ax.spines:
        ax.spines[spine].set_visible(False)

    return fig


def _get_extent_config(extent: UnitedState | Literal["CONUS"] | Extent, scale_win: int | None) -> tuple[Extent, int]:
    if isinstance(extent, Extent):
        return extent, scale_win or 1

    if extent == "CONUS":
        extent = Extent((20, 55), (-125, -60))
        scale_win = scale_win or 10
        return extent, scale_win

    elif extent in STATE_BOUNDS:
        extent = STATE_BOUNDS[extent]
        scale_win = scale_win or 5
        return extent, scale_win

    raise ValueError(f"{extent:=} not found. Valid options are `CONUS` or two-letter state codes (uppercase).")


@unzip_if_gz
def precip_rate_map(
    file: PathLike,
    state: UnitedState | Literal["CONUS"] | Extent,
    scale_win: int | None = None,
    _plt_close: bool = True,
) -> plt.Figure:
    """
    Make a map of precipitation rate.

    Parameters
    ----------
    file : Path
        Path to a file containing precipitation rate data.
    state : US_State | Literal["CONUS"]
        State to plot. If "CONUS", the entire CONUS is plotted.
    scale_win : int | None = None
        Window size for downscaling. If `None`, it defaults to 5 for states and 10 for CONUS.
    _plt_close: bool = True
        Close plot to avoid duplicates in Notebooks. Set to `false` when testing because
        matplotlib.testing.image_comparison expects an open plot.

    Returns
    -------
    plt.Figure
        Figure object. It uses the `cartopy` library to plot the map.
    """
    file = Path(file)

    if DATA_NAMES["precip_rate"] not in file.name:
        raise ValueError(f"File {file} does not contain **precipitation rate** data.")

    extent, scale_win = _get_extent_config(state, scale_win)

    with xr.open_dataset(file, engine="cfgrib", decode_timedelta=False) as ds:
        # -> Clip to extent
        # |-> Mask out no data (-3 for precipitation data)
        # |-> Hide small intensities (PrecipRate < 1)
        # |-> Downscale to coarser resolution
        # |-> Calculate mean over coarsened array

        xclip = ds.loc[extent.as_xr_slice()]
        masked = xclip.where(xclip["unknown"] != -3).where(xclip["unknown"] >= 1)
        coarse = masked.coarsen(latitude=scale_win, longitude=scale_win, boundary="pad").mean()

        # Make figure
        fig = _make_fig(coarse["unknown"], extent=extent, data_type="precip_rate")

    if _plt_close:
        plt.close()
    return fig


@unzip_if_gz
def precip_flag_map(
    file: PathLike,
    state: UnitedState | Literal["CONUS"] | Extent,
    scale_win: int | None = None,
    _plt_close: bool = True,
) -> plt.Figure:
    """
    Make a map of precipitation types.

    Parameters
    ----------
    file : Path
        Path to a file containing precipitation rate data.
    state : US_State | Literal["CONUS"]
        State to plot. If "CONUS", the entire CONUS is plotted.
    scale_win : int | None = None
        Window size for downscaling. If `None`, it defaults to 5 for states and 10 for CONUS.
    _plt_close: bool = True
        Close plot to avoid duplicates in Notebooks. Set to `false` when testing because
        matplotlib.testing.image_comparison expects an open plot.

    Returns
    -------
    plt.Figure
        Figure object. It uses the `cartopy` library to plot the map.
    """
    file = Path(file)

    if DATA_NAMES["precip_flag"] not in file.name:
        raise ValueError(f"File {file} does not contain **precipitation flag** data.")

    extent, scale_win = _get_extent_config(state, scale_win)

    with xr.open_dataset(file, engine="cfgrib", decode_timedelta=False) as ds:
        # - Mask out no data (-3 for precipitation data)
        # |-> Mask out no rain (Flag = 0)
        # |-> Clip to extent
        # |-> Downscale to coarser resolution
        # |-> Calculate mode over coarsened array
        masked = ds.where(ds["unknown"] != -3).where(ds["unknown"] != 0)
        xclip = masked.loc[extent.as_xr_slice()]
        coarse = xclip.coarsen(latitude=scale_win, longitude=scale_win, boundary="pad").reduce(
            lambda x, axis: stats.mode(x, axis=axis).mode
        )

        fig = _make_fig(coarse["unknown"], extent=extent, data_type="precip_flag")

    if _plt_close:
        plt.close()
    return fig
