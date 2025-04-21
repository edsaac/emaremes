.. emaremes documentation master file, created by
   sphinx-quickstart on Tue Apr 15 08:29:30 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

`emaremes`
========================

.. note::
   This project is under active development.


`emaremes` is a Python library that provides tools for accessing and handling meteorological data from the 
the `MRMS (Multi-Radar/Multi-Sensor) system <https://www.nssl.noaa.gov/projects/mrms/>`_. It facilitates the downloading and processing of high-resolution 
weather radar data, particularly useful for analyzing precipitation intensity and accumulation.


.. figure:: _static/test_plotting/CONUS_precip_flag.png
   :width: 100%
   :alt: CONUS Map for Precipitation type on Sep 27th, 2024

   CONUS Map of precipitation type on Sep 27th, 2024 (Hurricane Helene)


`emaremes` is divided in three modules that cover functions for...

- ... downloading and organizing GRIB files locally
- ... building time series from points or polygons
- ... plotting maps 


.. seealso::
   `Iowa State Mesonet Website <https://mesonet.agron.iastate.edu/archive/>`_
      This page contains the archive of MRMS resources that :py:mod:`emaremes` fetches.

   `NOAA - MRMS Quantitative Precipitation Estimates <https://inside.nssl.noaa.gov/mrms/>`_
      The official website of the MRMS-QPE system.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   install
   how-to
   api