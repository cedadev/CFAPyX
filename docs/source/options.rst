========================
cfapyx Usage and Options
========================

**New from Sept 2026**
----------------------

The CFA engine now utilises the attributes ``max_request_block`` and ``batch_request_size`` to customise how requests are made to remote servers in the pyfive backend. Add these to the other ``cfa_options`` to enable when reading remote files.

The ``batch_request_size`` specifies the number of individual requests that can be made asynchronously in parallel by the backend session. Some remote services providing data access have limits applied to the rate of requests, such that if you attempt to make more than a certain number of requests simultaneously (or if you have more than that number of open/pending requests) you may be denied further requests with a ``HTTP 429: Too Many Requests`` error. If you encounter this problem, it may be effective to simply limit the requests your own client can make concurrently.

In addition to the above limit, it may be more efficient for you to make fewer requests in general as each request will require some time for handling on the server end. If you use the ``max_request_block`` parameter, the backend part of the pyfive client will attempt to combine requests that represent a continuous block of memory, so instead of making many requests you can just make one larger request and reduce the overheads. Range-requests will be combined up to the limit you set here (10 Megabytes in the example above). For maximum efficiency, consider the bandwidth available to you, in relation to the maximum request size and the number of concurrent requests. The above example of 80 requests at 10MB per request, when combined with a typical non-fibre bandwidth of 30 Mbps would mean the whole batch would take 200 seconds which may be more than the timeout limit for the session, so you may want to reduce the number of requests or the block size for your situation.

General Use
-----------

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
