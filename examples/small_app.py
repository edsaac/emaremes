import emaremes as mrms
import streamlit as st
from pandas import Timestamp, Timedelta

st.set_page_config(page_title="MRMS Data", layout="centered")

st.title("MRMS Data")
st.write("This is a small app to test the `emaremes` package.")

if "files" not in st.session_state:
    files = mrms.fetch.timerange(
        initial_datetime=Timestamp("2024-09-26"),
        end_datetime=Timestamp("2024-09-27"),
        frequency=Timedelta("1h"),
        data_type="precip_rate",
    )
    st.session_state["files"] = files

files = st.session_state["files"]

with st.sidebar:
    state = st.selectbox("Select a state:", options=mrms.utils.STATE_BOUNDS.keys(), index=0)

file = st.select_slider(
    "Select a file:",
    options=files,
    value=None,
    format_func=lambda x: Timestamp(
        x.name.replace("PrecipRate_00.00_", "").replace(".grib2.gz", "")
    ),
)

if file:
    fig = mrms.plot.precip_rate_map(file, state=state, scale_win=10)
    st.pyplot(fig, use_container_width=True)
