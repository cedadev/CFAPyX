.. kerchunk*builder documentation master file, created by
   sphinx*quickstart on Thu Jan 25 10:40:18 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

CFAPyX Documentation
====================

**CFAPyX** is an Xarray add-on module that enables the **CFA-netCDF xarray engine** in python. 

CFAPyX follows the `CFA-conventions <https://github.com/NCAS-CMS/cfa-conventions/blob/main/source/cfa.md>`_ for properly formatting a CFA-netCDF file to access distributed netCDF data.

The package can be enabled in xarray when opening a CFA file using the `engine="CFA"` keyword argument. This will decode your CFA-netCDF file into a properly formatted Xarray Dataset, which will lazily load your data from distributed sources when reqired for computation or plotting.

Current support is limited to local netCDF4 formatted files, but future additions will provide access to:
 - UM and PP files
 - S3 interface to files in Object Storage.

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   Inspiration for CFA <inspiration>
   Xarray Engine Overview <overview>
   Fragments in CFA <fragments>
   CFA Backend Entrypoint <backendentrypoint>
   CFA DataStore <datastore>
   CFA Fragment Array <fragmentarray>
   Fragment/Chunk Objects <chunkwrapper>
   NetCDF group handling <groups>
   
Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Acknowledgements
================
CFAPyX is being developed as part of a joint project between the Centre for Environmental Data Analysis (CEDA) and the NCAS Computational Modelling Services team (NCAS-CMS)

.. image:: _images/ceda.png
   :width: 300
   :alt: CEDA Logo

.. image:: _images/ncas.png
   :width: 300
   :alt: NCAS Logo