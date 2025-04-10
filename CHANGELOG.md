# ⭐ 0.0.4 | 2024-04-10
- Rename `mrms.download` to `mrms.fetch`
- Consolidate unzip decorator to handle both `.grib2.gz` and `.grib2` files.
- Add other data sources from MRMS: precipitation rate, precipitation flag, and 1h, 24h and 72h accumulated precipitation. 
- Add example notebooks. (:TODO clean up carpentry folder)
- Calculate mode when plotting coarsed precipitation flag data.

___________

## Previous notes:

**0.0.3 | 2024-03-25**
- Organized timeseries building functions into the ts submodule.
- Rename `mrms.timeseries` to `mrms.ts`.
- Add `mrms.plot` submodule.
- Build timeseries using polygons.


**0.0.2 | 2024-03-20**
- Add `mrms.timeseries` tools. For larger datasets, these run faster than using `xr.open_mfdataset`.
- Add docstrings to functions.

**0.0.1 | 2024-03-19**
- Initial release.
- Add `mrms.download_data` helper.