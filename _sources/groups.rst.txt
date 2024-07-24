=================================
Handling netCDF4 Groups in CFAPyX
=================================

.. Note::

    The Xarray package currently does not handle multiple NetCDF groups in the same way as other packages like
    cf-python. In Xarray, a group can be extracted from a NetCDF file as an enclosed `Dataset` object, but typically
    only the ``group`` content is retrieved. Any so-called ``global`` dimensions/attributes are not loaded.

    In CFAPyX, the ``global`` and ``group`` parameters are both loaded into the final dataset, such that any parameters
    which are shared between groups can be accessed. This is vital in particular for decoding grouped aggregated variables,
    where the aggregated variables link to global parameters.

In order to facilitate loading both ``global`` and ``group`` parameters from the CFA dataset, a small change to the standard
``NetCDF4ArrayWrapper`` class from Xarray, where an attempt is made to load the variable from the ``group`` dataset first, but
if this is unsuccessful then the ``global`` dataset is used.

.. automodule:: CFAPyX.group
    :members: