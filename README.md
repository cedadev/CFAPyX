# CFAPyX
CFA python Xarray module for using CFA files with xarray.

## Installation of requirements

```
# If using a venv
conda install -c conda-forge udunits2

pip install cf-python cf-plot
# and/or
conda install -c conda-forge cf-python cf-plot
```

## 08/07/2024 - Setup and first attempts to use cf-python

- Successfully generated a CFA file from 5 CMIP6 files.
- Data duplicated into a single combined file, slightly larger than the sum of the initial set of files combined.
- Combined dataset can be opened with Xarray (no surprise there since this is just a combined NetCDF file)

First goal is to get to grips with cf.aggregate in order to produce `domain-only` aggregations that can be interpreted
by cf-python (should be MUCH smaller than any single individual file.)

- NOTE: cf.aggregate and cfa are NOT the same. Aggregations simply combine sets of data, while this investigations specifically look at 'virtual aggregations'

## 09/07/2024 - Attempting to Aggregate CMIP data

- Running cf.write with cfa engaged using a sample set of CMIP files as a test to see how a sample file will be aggregated.
- Cf generally failed to virtually aggregate CMIP example datasets
- Issues with installing editable packages on Jasmin solved in a 'hacky' way with local versions and a template for new tests

## 10/07/2024 - Getting installed editable packages.

Notes inside cf_repos.README

## 11/07/2024 - Xarray Insertion

Notes inside xarray_repos.README

Simplest implementation of CFA to Xarray Dataset could be:
 - Open with xr.open_dataset and post-process inside xarray backend
 - Dive into xarray netcdf backend to find h5py connection and go from there.

See xarray_CFA.py for implementation details and Xarray classes to use as insertion points for CFA decoder functionality.

## 12/07/2024 - CFA Decoding in cf-python

`cf.read_write.netcdf.NetCDFRead` class has a `read()` method, likely linked from `cf.read()` on determining the file format. This class also contains routines for:
 - `_create_cfanetcdfarray`
 - link to `implementation.initialise_CFANetCDFArray`: 695

Leads directly to `CFANetCDFArray` class instantiation - test with example data.

`NetCDFRead._create_cfanetcdfarray` returns a pre-decoded array object. This must lead later to a decode step to fetch the data and build the array :697

Found CFANetCDFArray to Dask Array decoder in `to_dask_array`: 697 (cfanetcdfarray.py)
 - FragmentArray (fragment.NetCDFFragmentArray) takes the parameters:
 ```
 filename = 'fragment_file.nc'
 address = 'variable_name'
 dtype = dtype('float64')
 shape = tuple(2, 180, 360) # Fragment size
 aggregated_units = Units # CF type
 aggregated_calendar = ?

# See netcdffragmentarray.py: 27-67 for more details.
 ```

FragmentArray creates an array object from the selected fragment file. Will need a reduced implementation of this within the xarray backend (since it is embedded into cf-python)

## 15/07/2024

Actual decoding function takes place in the __init__ routine for CFANetCDFArray (from 137)

Variables need to be translated from netcdf4.Variable to something else up to 164, happens earlier in the process:
 - 656: cf.read_write.netcdf.netcdfread.NetCDFRead (read_vars)
   - Already decoded netcdf variables into cfa_aggregation_instructions composite set of variables.
 - Up to 1327 in NetCDFRead, variable_attributes contains blank cfa_attributes.

## 16/07/2024

Summary of current structure:
 - Normal variables are loaded by the CFADataStore using LazilyIndexedArray with a NetCDF4ArrayWrapper as usual.
 - Aggregated variables are loaded using a FragmentArrayWrapper instead.
   - Requires the decoded_cfa instructions which are loaded by the CFADataStore.

 - The FragmentArrayWrapper contains the aggregation instructions in the form of aggregated_data.
   - Individual files should only be loaded when absolutely required.
   - Uses the get_array function to load a dask array of only the required data fragments.
   - FragmentArray should only be a `data` accessor, so there is no need for metadata about the variable/domain etc.

### Things needed in the get_array function:
 - calendar (maybe)
 - chunks - subarray_shapes
 - NetCDFFragmentArray - DataStore?
 - subarrays (maybe)
 - dask array build using a very strange dictionary
sd

## 17/07/2024

Fixed the numpy datatype issue (wrong object being used to define dtype)

### Custom Fragment Arrays

- Requires __getitem__ and get_array for selecting some data.
- Uses netCDF4 library (for NetCDFFragmentWrapper) to load only data required.
- Slicing takes place BEFORE loading as a numpy array since this is then the required data.
- All accessing handled by dask array as part of xarray.Variable.array component.

## 22/07/2024

Experiments with substitutions and other cfa options.
- Now applied when creating the fragment array so the changes apply when fetching aggregated data.

Note: Adding methods to the xarray.Dataset or any subcomponent does not look feasible, therefore CFA attributes or metadata should either
be explicitly defined as attributes of the xarray variable on definition, or they should be considered hidden for the xarray user. This also
means xarray should not be used to create/edit CFA files.

### CFA-Compliance Additions to CFAPyX

- Handler for extra terms in aggregated data for a specific variable.
- Baked-in substitutions into the aggregation definition variables.
- Handler for 'group' based aggregations where all the aggregated metadata is stored within a group.
- Coordinate variables additional handling (if necessary)

Hierarchical structure of nested groups (netCDF4 implementation) is not supported in Xarray. The current 'group' implementation has very limited functionality:
 - Opening a group from a dataset is possible with xarray only if the group is effectively a complete picture of the variable in question.
 - You are also required to know the group identifier as xarray will not show these.
 - For CFA-netCDF files, xarray groups will only work if both the `aggregated` and `normal` dimensions are contained within the group.
 - Examples 4 and 5 from the CFA conventions (https://github.com/NCAS-CMS/cfa-conventions) **will not open properly** with the current implementation.

A bespoke implementation could use:
 - A global attribute in the netCDF which lists the groups, such that the user could pick a group from there (no good for already-created CFA files.)

 - An expansion of the group structure into individual variables with a `group` or `source_group` attribute. This would remove the appearance of groups for a data user, unless we change the names of the grouped variables to be `<group_name>/<variable>` if that is allowed by xarray.

 - Alternatively investigate absorbing global variables into the group information when implementing in Xarray.

Summary of groups issue and solutions:
 - Xarray doesn't support non-complete groups (dimensions outside the group are ignored)

Solutions:
 - Ensure CFA stores complete groups in the next version.
 - Allow for xarray **group expansion** for viewing - xarray SHOULD NOT be used to create CFA files.

Current Implementation:
 - CFAPyX now uses `global` and `group` dimensions/attributes, so you can request a specific group as normal, but the `global` dimensions are also loaded as if they were part of the group.


## 23/07/2024

Fixes to the FragmentArray class where there were hardcoded values for fragment_dims and ndim, should now mean slicing works properly and across multiple fragmented dimensions.

Performance Testing with Kerchunk shows CFA-netCDF is significantly faster to load (~50%) and is comparable for calculation and plot times, although note here the Kerchunk Engine Lazy Loading issue (https://github.com/fsspec/kerchunk/issues/461). The older method does lazy loading correctly but is slower in all categories.

Next steps:
 - Handler for extra terms
 - Baked-in substitutions
 - Coordinate variables additional handling
 - Tidy-up/Repo sort
 - Documentation init.