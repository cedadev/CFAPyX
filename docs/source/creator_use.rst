===========
CFA Creator
===========

The cfapyx engine also contains a class for use when creating ``CFA-netCDF`` files. These automatically follow the Aggregation conventions for CF-1.12, and the aggregation files produced will have this as the CF convention value in conventions.

.. note::

    CF Convention 1.12 is added to the aggregation file, but only applies to the aggregation file and not the fragment files. For fragment files compliant at a higher convention than 1.12, this convention will be carried over to the aggregation file, but as a minimum all files produced by the Creator tool will be 1.12 - this also enables interpreting of these files by the CFDM at that version and also cf-python.

Logging
-------

To enable the logger for cfapyx, import the logging package and set the basic config for logging operations. Log level and log message format should be specified here.

::

    import logging
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s [%(name)s]: %(message)s')


Create
------

To then use the creator functionality from this package, use the following.

::

    from cfapyx import CFANetCDF

    ds = CFANetCDF(
        set_of_files, # The set of files to be aggregated
        concat_msg = 'See individual files for more details', # Replaces attributes that differ
    )

The aggregated dataset can then be created using the ``create`` method. Additional kwargs are:
 - updates: Update the values of global attributes with new values.
 - removals: Remove/Ignore some attributes in the Aggregated file.
 - agg_dims: If the aggregation dimensions are known, state them here. This will improve performance if there are many dimensions that are not aggregated.

::

    ds.create(
        updates = {'tracking_id':'1'},
        removals = ["PI's dog's name"],
        agg_dims = ['time']
    )

It is advised that you check how variables and dimensions have been arranged by ``cfapyx``, which are evident from the properties below:

::
    
    ds.agg_dims 
    #  - The dimensions that are aggregated across the set of files
    ds.coord_dims 
    #  - Dimensions which also link to a variable (e.g lat/lon)
    ds.pure_dims 
    #  - Dimensions which have a size but no variable/array component.

    ds.aggregated_vars 
    #  - Variables which change across the aggregation dimension(s)
    ds.identical_vars 
    #  - Variables which do not change across the aggregation dimension(s)
    ds.scalar_vars 
    #  - Single-valued variables with no dimensions.


Write
-----

Finally the created dataset can be written to a CFA-netCDF file using the following.

::

    ds.write(output_file)

This file may be read into Xarray as a familiar xarray dataset with:

::
    
    xarray_ds = xarray.open_dataset(output_file, engine='CFA')

Where the engine is required to decode the aggregation instructions contained in the ``CFA-netCDF`` file. Note that 
without this engine the aggregation instructions will be displayed but not decoded.

Extension/Parallelisation
-------------------------

The ``CFANetCDF`` object now supports the extension/concatenation of multiple aggregation files, enabling the use of parallelisation. Here the syntax ``extension`` applies to dimensions that are already aggregated and will be ``extended`` by combining multiple aggregations. All aggregation dimensions must be already established in each partial aggregation file (i.e all files must be time-aggregated or otherwise), as the functionality to aggregate per-dimension in parallel is not supported.

To make use of this functionality, simply apply the create/write process above to batches of fragment files at any size required, then feed the resulting aggregation files back into a new ``CFANetCDF`` creator object to merge the aggregations.

::

    from cfapyx import CFANetCDF

    ds = CFANetCDF(
        set_of_agg_files, # The set of aggregation files to be combined/extended
        concat_msg = 'See individual files for more details', # Replaces attributes that differ
    )

Given a sufficient setup with use of parallel jobs (and a dependent final job) it is now possible to parallelise the aggregation process to an arbitrary arrangement of jobs. 

.. note:: 

    For the most time-efficient parallel arrangement, use file batches of size ``root(n)`` where n is the total number of files. This creates a set of ``root(n)`` partial aggregation files, which then minimises the effective total aggregation time to just ``2*root(n)``.