from pathlib import Path
from functools import partial
from math import isclose as _isclose

import pytest
import geopandas as gpd
from pandas import Timestamp, Timedelta
from shapely.geometry import Polygon

import emaremes as mrms


isclose = partial(_isclose, abs_tol=1e-4)


@pytest.fixture
def gzfiles(tmp_path):
    mrms.fetch.path_config.set_prefered(tmp_path)

    # Hourly data on a day during Hurricaine Helene
    gzfiles = mrms.fetch.timerange(
        "2024-09-26T12:00:00",
        "2024-09-28T00:00:00",
        frequency=Timedelta(minutes=60),
        verbose=True,
    )

    return gzfiles


@pytest.fixture
def polygon_geodf() -> gpd.GeoDataFrame:
    """Define a dummy GeoDataFrame with three rectangles"""
    polygons = {
        "Rect_0": Polygon.from_bounds(-85, 38, -83, 40),
        "Rect_1": Polygon.from_bounds(-85, 35, -83, 37),
        "Rect_2": Polygon.from_bounds(-85, 32, -83, 34),
    }

    gdf = gpd.GeoDataFrame(
        polygons.keys(),
        geometry=list(polygons.values()),
        columns=["Rect"],
        crs="EPSG:4326",
    )

    gdf.set_index("Rect", inplace=True)
    return gdf


def test_polygon_timeseries(gzfiles: list[Path], polygon_geodf: gpd.GeoDataFrame):
    file = gzfiles[23]
    time, val = mrms.ts.polygon.query_single_file(file, polygon_geodf)
    assert Timestamp(time) == Timestamp("2024-09-27T11:00:00")
    assert isclose(val["Rect_0"], 1.74486)
    assert isclose(val["Rect_1"], 2.15366)
    assert isclose(val["Rect_2"], 3.30743)

    df = mrms.ts.polygon.query_files(gzfiles, polygon_geodf)
    assert isclose(df.sum()["Rect_0"], 59.5774)
    assert isclose(df.sum()["Rect_1"], 65.3580)
    assert isclose(df.sum()["Rect_2"], 119.2651)
