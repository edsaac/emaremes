.. emaremes documentation master file, created by
   sphinx-quickstart on Tue Apr 15 08:29:30 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

`emaremes`
========================

`emaremes` is a Python library that provides tools for accessing and handling meteorological data from the 
the `MRMS (Multi-Radar/Multi-Sensor) system <https://www.nssl.noaa.gov/projects/mrms/>`_`. It facilitates the downloading and processing of high-resolution 
weather radar data, particularly useful for analyzing precipitation intensity and accumulation.


.. image:: _static/test_plotting/CONUS_precip_flag.png
  :width: 100%
  :alt: Alternative text

`emaremes` is divided in three modules that cover functions for...

- ... downloading and organizing GRIB files locally
- ... building time series from points or polygons
- ... plotting maps 

.. note::
   This project is under active development.


Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   install
   how-to
   api

