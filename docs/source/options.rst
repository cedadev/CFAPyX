========================
cfapyx Usage and Options
========================

The cfapyx engine 'CFA' is enabled using the ``engine`` keyword to Xarray as follows:

::

    ds = xr.open_dataset(cfa_file, engine='CFA')

The ``ds`` object can then be used as normal in Xarray. Keyword arguments are provided to cfapyx with the following syntax:

::

    ds = xr.open_dataset(cfa_file, engine='CFA',
                        cfa_options = {
                                'substitutions' : {'base':'sub'},
                                'decode_cfa' : False,
                                'chunks' : {}
                            }
                        )

Three keyword arguments are currently supported within ``cfa_options``:
 - **Substitutions**: Additional substitutions provided to the CFA decoder for this file, following the CF 1.12 conventions 
   for syntax with 'base' and 'sub'.
 - **Decode CFA**: Optional parameter to disable decoding of aggregation variables if required. Default is True.
 - **Chunks**: Replaces the typical ``chunks={}`` normally provided to Xarray for Dask chunks. You can still use the normal 
   dask chunks keyword but may get better performance using CFA chunks because this takes into account the underlying storage 
   regime including Fragment extents. See the diagram in :ref:`Fragments, Chunks and Partitions` for more details. 

.. Note::
  
    The ``chunks`` option for cfapyx now includes an additional option: ``optimised``. This is an upgrade from the dask ``auto``
    chunks option, where the chunk size is automatically calculated by dask, then shifted such that the number of chunks approaches
    a power of 2. This has significant computational performance benefits.
