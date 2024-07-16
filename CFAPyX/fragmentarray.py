from CFAPyX.utils import OneOrMoreList

import dask.array as da
from dask.array.core import getter
from dask.base import tokenize

import numpy as np

class FragmentArrayWrapper():

    def __init__(self, decoded_cfa):

        self.aggregated_data = decoded_cfa['aggregated_data']
        self.fragment_shapes = decoded_cfa['fragment_shape']

        fragments      = list(self.aggregated_data.keys())
        self.sources   = [self.aggregated_data[i]['filename'] for i in fragments]
        self.names     = OneOrMoreList([self.aggregated_data[i]['address'] for i in fragments])

        # Required parameters list
        self.ndim = 0 # Current
        self.shape = (20,180,360)
        self.units = ''
        self.__array_function__ = None
        self.dtype = np.float64
        # ...

        self.arrays = []


    def _load_array(self):
        # Put into a _load function so this only happens at the point where you request some data

        # Need to know which fragments to load here, do not load every single one at all times.
        # May not even need to use a datastore, just load with the correct slicing.
        raise NotImplementedError

        """
        for i, s in enumerate(self.sources):
            store = NetCDF4DataStore.open(s)
            array = NetCDF4ArrayWrapper(self.names[i], store)

            self.arrays.append(array)
        return self.arrays
        """

    @property
    def shape(self):
        return self._shape
    
    @shape.setter
    def shape(self, value):
        self._shape = value

    @property
    def units(self):
        return self._units
    
    @units.setter
    def units(self, value):
        self._units = value

    def __getitem__(self, selection):
        return selection

    def get_array(self, needs_lock=True):

        name = (f"{self.__class__.__name__}-{tokenize(self)}",)

        dtype = self.dtype
        units = self.units

        calendar = None # Fix later

        aggregated_data = self.aggregated_data
        
        chunks = None

        # Ignore/remove the below code
        for s in self.arrays:
            # Cast this to numpy? *crossed fingers*
            var = np.array(s.get_array(needs_lock=needs_lock))

        # Should return a dask Array of just the fragments required here.