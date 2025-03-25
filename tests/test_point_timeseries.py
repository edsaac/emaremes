from emaremes.ts import extract_point_value, extract_point_series
from emaremes.download import LOCALPATH


def test_extract_point_value_from_gz():
    grib_files = (LOCALPATH / "20250101").glob("*.gz")
    grib_files = sorted(grib_files)
    file = grib_files[0]

    # A value out of range
    lat, lon = 40.0, -87.0
    time, val = extract_point_value(file, lat, lon)
    assert val == -3.0  # -3 means value out of range

    # A value in range but no precipitation
    lon += 360
    time, val = extract_point_value(file, lat, lon)
    assert val == 0.0
