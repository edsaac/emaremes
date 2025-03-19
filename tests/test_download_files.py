import pandas as pd
import pytest

from emaremes.download_data import GribFile, download_file, download_timerange


def test_download_file():
    # Valid timestamp
    tstamp = pd.Timestamp("2025-01-01T12:00:00")
    gfile = GribFile(tstamp)
    download_file(gfile)

    assert gfile.exists()

    # Bad but valid timestamp
    tstamp = pd.Timestamp("2025-01-01T13:12:47")
    gfile = GribFile(tstamp)
    download_file(gfile)

    assert gfile.exists()

    # Bad timestamp
    tstamp = pd.Timestamp("2025-01-01T13:11:00")
    with pytest.raises(ValueError):
        gfile = GribFile(tstamp)


def test_download_range():
    init_tstamp = pd.Timestamp("2025-02-02T12:00:00")
    end_tstamp = pd.Timestamp("2025-02-02T13:00:00")

    gfiles = download_timerange(init_tstamp, end_tstamp)
    for gf in gfiles:
        assert gf.exists()
