===============
Release History
===============

Release Notes 1.0.2
-------------------
- Fixed an bug with the pyfive data extractor returning data instead of setting to the correct private variable.

Release Notes 1.0.1
-------------------
- Fixed an issue handling the multiple file openers where only the last error is raised. Now all errors are listed and a general error is raised.

Release Notes Version 1.0.0
---------------------------
- The segmentation fault issue with netCDF4 filehandlers has been resolved using a global lock on NetCDF4 data access.
- NetCDF4 and Pyfive are both supported, with pyfive used for remote file access. This has a limitation on NetCDF3 files which are not compatible with pyfive.
- Unit conversions are now automatic when reading/writing CFA files, this is no longer a fatal error when creating the dataset. This is also accounted for when extending an existing aggregation by adding more files, although prepending files before the current aggregation is not supported.
- Various xarray/dask computation anomalies have been fixed, where it was sometimes required to run .compute() multiple times to receive data.
- ArrayPartition module is no longer a dependency as the partition module has been migrated to this package.


2026 Pre-Production Release
---------------------------
- Revised handling of ``scale_factor`` and ``add_offset`` properties. These are now handled by individual fragments,
but the scaling/offsetting can still be deactivated via the xarray dataset instantiation, this now filters all the way down to the partition-level.
- Fixed bugs relating to single-indexes. Single-valued slices like ``slice(0,1)`` and single-indexes like ``0`` are now distinctly handled internally,
by adjusting the ``extent`` property of partitions. The extent on an indexed dimension remains at size ``1`` but the ``shape`` parameter generates without that dimension.
This is handled internally by skipping the lost dimension in all cases except for when data is accessed, in which case the entire extent is applied, including the single-index value that is skipped in other operations.
