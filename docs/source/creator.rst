===========
CFA Creator
===========

The CFAPyX engine also contains a class for use when creating ``CFA-netCDF`` files. These automatically follow the Aggregation conventions for CF-1.12.

Logging
-------

To enable the logger for CFAPyX, import the logging package and set the basic config for logging operations. Log level and log message format should be specified here.

::

    import logging
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s [%(name)s]: %(message)s')


Create
------

To then use the creator functionality from this package, use the following.

::

    from CFAPyX import CFANetCDF

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

Finally the created dataset can be written to a CFA-netCDF file using the following.

::

    ds.write(output_file)