import pandas as pd
import pytest
import emaremes as mrms
from typing import get_args


def test_GribFile(tmp_path):
    # Set tmp_path as the default path
    mrms.fetch.path_config.set_prefered(tmp_path)

    ## Valid timestamp
    tstamp = pd.Timestamp("2024-10-01T12:00:00")
    gfile = mrms.fetch._GribFile(tstamp)
    mrms.fetch._single_file(gfile)
    assert gfile.exists()

    # Try to download the same file again (it should not)
    mrms.fetch._single_file(gfile)
    assert gfile.exists()

    ## Bad but valid timestamp
    tstamp = pd.Timestamp("2024-11-01T13:12:47")
    gfile = mrms.fetch._GribFile(tstamp)
    mrms.fetch._single_file(gfile)
    assert gfile.exists()

    ## Bad timestamp
    tstamp = pd.Timestamp("2024-09-15T13:11:00")
    with pytest.raises(ValueError):
        gfile = mrms.fetch._GribFile(tstamp)


def test_mrms_datatypes(tmp_path):
    # Set tmp_path as the default path
    mrms.fetch.path_config.set_prefered(tmp_path)

    ## Precipitation rate
    tstamp = pd.Timestamp("2024-06-27T16:12:00")
    gfile = mrms.fetch._GribFile(tstamp)
    mrms.fetch._single_file(gfile)
    assert gfile.exists()

    ## The other data types
    for dtype in get_args(mrms.typing_utils.MRMSDataType):
        gfile = mrms.fetch._GribFile(tstamp, data_type=dtype)
        mrms.fetch._single_file(gfile)
        assert gfile.exists()


def test_download_range(tmp_path):
    # Set tmp_path as the default path
    mrms.fetch.path_config.set_prefered(tmp_path)

    init_tstamp = "2025-02-02T12:00:00"
    end_tstamp = "2025-02-02T13:00:00"

    gfiles = mrms.fetch.timerange(init_tstamp, end_tstamp)
    for gf in gfiles:
        assert gf.exists()
