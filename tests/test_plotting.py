from pathlib import Path
import pytest
import matplotlib.pyplot as plt
from matplotlib.testing.decorators import image_comparison


import emaremes as mrms

image_comparison_kwargs = dict(
    remove_text=False,
    extensions=["png"],
    savefig_kwarg={"bbox_inches": "tight"},
)


@pytest.fixture
def precip_rate_file(tmp_path: Path) -> Path:
    mrms.fetch.path_config.set_prefered(tmp_path)
    files = mrms.fetch.timerange("20240927-120000", "20240927-120000", data_type="precip_rate")
    return files[0]


@pytest.fixture
def precip_flag_file(tmp_path: Path) -> Path:
    mrms.fetch.path_config.set_prefered(tmp_path)
    files = mrms.fetch.timerange("20240927-120000", "20240927-120000", data_type="precip_flag")
    return files[0]


############################


@image_comparison(baseline_images=["NC_precip_rate"], **image_comparison_kwargs)
def test_nc_preciprate(precip_rate_file: Path) -> plt.Figure:
    fig = mrms.plot.precip_rate_map(precip_rate_file, state="NC")
    return fig


@image_comparison(baseline_images=["CONUS_precip_rate"], **image_comparison_kwargs)
def test_conus_preciprate(precip_rate_file: Path) -> plt.Figure:
    fig = mrms.plot.precip_rate_map(precip_rate_file, state="CONUS")
    return fig


@image_comparison(baseline_images=["NC_precip_flag"], **image_comparison_kwargs)
def test_nc_precipflag(precip_flag_file: Path) -> plt.Figure:
    fig = mrms.plot.precip_flag_map(precip_flag_file, state="NC")
    return fig


@image_comparison(baseline_images=["CONUS_precip_flag"], **image_comparison_kwargs)
def test_conus_precipflag(precip_flag_file: Path) -> plt.Figure:
    fig = mrms.plot.precip_flag_map(precip_flag_file, state="CONUS")
    return fig
