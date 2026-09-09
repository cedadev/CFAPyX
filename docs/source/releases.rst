===============
Release History
===============

Release Notes 2026.9.10
----------------------
Github Copilot sanity checks identified specific bugs in the package:
- Correct_slice function had incorrect usage of extent - this was introduced when this transitioned from a method to a function.
- fixed an issue where DS could be undefined in opening a fragment if all options failed.
- Fixed default mutable arguments to functions in a few specific places.

Release Notes 2026.9.9
----------------------
- Fixed syntax issue with units handling that bypassed test suite.
- Removed substitutions attribute when using creator - this is now handled as a one-off change for the file locations.

Release Notes 2026.9.8
----------------------
- Added deprecation warning for using multiple file versions when creating CFA-NetCDF files as these have been removed from the official conventions.
- Added remote CFA-NetCDF fetch when opening the top-level manifest files. CFA reading can now be done entirely remotely.

Release Notes 2026.9.7
----------------------
- Reverted back to old date-based versioning (issues with pypi listing order)
- Fixed client connections issues with using pyfive and opening too many sessions.

Release Notes 1.0.3
-------------------
- Fixed another bug in pyfive with unit extraction.

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
