from pathlib import Path
from math import isclose

import pytest
import geopandas as gpd
from pandas import Timestamp, Timedelta
from shapely.geometry import Point

import emaremes as mrms


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
def airport_geodf():
    """Define a dummy GeoDataFrame with airport names and their coordinates"""

    airports = {
        "Asheville Regional Airport": Point(-82.541, 35.436),
        "Jacksonville International Airport": Point(-81.689, 30.494),
        "Hartsfield-Jackson Atlanta International Airport": Point(-84.428, 33.641),
    }

    # Create a GeoDataFrame
    gdf = gpd.GeoDataFrame(
        airports.keys(),
        geometry=list(airports.values()),
        columns=["Airport Name"],
        crs="EPSG:4326",
    )
    gdf["Code"] = ["AVL", "JAX", "ATL"]
    gdf.set_index("Code", inplace=True)

    return gdf


def test_point_timeseries(gzfiles: list[Path], airport_geodf: gpd.GeoDataFrame):
    file = gzfiles[15]
    time, val = mrms.ts.point.query_single_file(file, airport_geodf)
    assert Timestamp(time) == Timestamp("2024-09-27T03:00:00")
    assert isclose(val["AVL"], 8.0)
    assert isclose(val["JAX"], 0.0)
    assert isclose(val["ATL"], 7.5)

    df = mrms.ts.point.query_files(gzfiles, airport_geodf)
    assert isclose(df.sum()["AVL"], 138.5)
    assert isclose(df.sum()["JAX"], 6.2, abs_tol=1e-6)
    assert isclose(df.sum()["ATL"], 249.0)
