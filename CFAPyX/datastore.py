from xarray.backends import ( 
    NetCDF4DataStore
)

from xarray.core.utils import FrozenDict
from xarray.core import indexing
from xarray.coding.variables import pop_to

from xarray.core.variable import Variable

import xarray


import netCDF4
import numpy as np
import os

from CFAPyX.utils import _ensure_fill_value_valid
from CFAPyX.fragmentarray import FragmentArrayWrapper
from CFAPyX.decoder import chunk_locations, chunk_positions

from CFAPyX.group import GroupedDatasetWrapper


xarray_subs = {
    'file:///':'/'
}

class CFADataStore(NetCDF4DataStore):
    """
    DataStore container for the CFA-netCDF loaded file. Contains all unpacking routines directly 
    related to the specific variables and attributes, but uses CFAPyX.utils for some of the aggregation
    metadata decoding.
    """

    def _acquire(self, needs_lock=True):
        """
        Fetch the global or group dataset from the Datastore Caching Manager (NetCDF4)
        """
        with self._manager.acquire_context(needs_lock) as root:
            ds = GroupedDatasetWrapper.open(root, self._group, self._mode)

        return ds

    def get_variables(self):
        """
        Fetch the netCDF4.Dataset variables and perform some CFA decoding if necessary.
        .. Note:: 
        
            ``ds`` is now a ``GroupedDatasetWrapper`` object from ``CFAPyX.group`` which has flattened 
            the group structure and allows fetching of variables and attributes from the whole group tree
            from which a specific group may inherit.

        :returns:       A ``FrozenDict`` Xarray object of the names of all variables, and methods to fetch those
                        variables, depending on if those variables are standard NetCDF4 or CFA Aggregated variables.
        """

        xarray_vars = {}
        r = {} # Real size of dimensions for aggregated variables.

        if not self.decode_cfa:
            return FrozenDict(
                (k, self.open_variable(k, v, r)) for k, v in self.ds.variables.items()
            )

        ## Proceed with decoding CFA content.

        if not hasattr(self, '_decoded_cfa'):
            self.perform_decoding()

        standardised_terms = (
            "cfa_location",
            "cfa_file",
            "cfa_address",
            "cfa_format"
        )

        ## Decide which dimensions and variables can be ignored when constructing the CFA Dataset.


        ## Obtain the list of fragmented dimensions and their real sizes.
        for dimension in self.ds.dimensions.keys():
            if 'f_' in dimension and '_loc' not in dimension:
                real_dim = dimension.replace('f_','')
                r[real_dim] = self.ds.dimensions[real_dim].size

        ## Ignore variables in the set of standardised terms.
        for avar in self.ds.variables.keys():
            cfa = False
            ## CF-Compliant method of identifying aggregated variables.
            if hasattr(self.ds.variables[avar], 'aggregated_dimensions'):
                cfa = True

            if avar not in standardised_terms:
                xarray_vars[avar] = (self.ds.variables[avar], cfa)

        return FrozenDict(
            (k, self.open_variable(k, v, r)) for k, v in xarray_vars.items()
        )

    def get_attrs(self):
        """
        Produce the FrozenDict of attributes from the ``NetCDF4.Dataset`` or ``GroupedDatasetWrapper`` in 
        the case of using a group or nested group tree.
        """
        return FrozenDict((k, self.ds.getncattr(k)) for k in self.ds.ncattrs())

    @property
    def cfa_options(self):
        """Property of the datastore that relates private option variables to the standard ``cfa_options`` parameter."""
        return {
            'substitutions': self._substitutions,
            'decode_cfa': self._decode_cfa
        }

    @cfa_options.setter
    def cfa_options(self, value):
        self._set_cfa_options(**value)

    def _set_cfa_options(
            self, 
            substitutions=None, 
            decode_cfa=True
        ):
        """
        Method to set cfa options.

        :param substitutions:           (dict) Set of provided substitutions to Xarray, following the CFA 
                                        conventions on substitutions.

        :param decode_cfa:              (bool) Optional setting to disable CFA decoding in some cases, default
                                        is True.
        """

        self._substitutions = substitutions
        self._decode_cfa    = decode_cfa

    @property
    def decode_cfa(self):
        return self._decode_cfa

    def open_variable(self, name: str, var, real_agg_dims):
        """
        Open a CFA-netCDF variable as either a standard NetCDF4 Datastore variable or as a
        CFA aggregated variable which requires additional decoding.

        :param name:        (str) A named NetCDF4 variable.

        :param var:         (obj) The NetCDF4.Variable object or a tuple with the contents
                            ``(NetCDF4.Variable, cfa)`` where ``cfa`` is a bool that determines
                            if the variable is a CFA or standard variable.

        :param real_agg_dims:       (dict) Named fragment dimensions with their corresponding sizes in 
                                    array space.

        :returns:       The variable object opened as either a standard store variable or CFA aggregated variable.
        """
        if type(var) == tuple:
            if var[1] and self.decode_cfa:
                variable = self.open_cfa_variable(name, var[0], real_agg_dims)
            else:
                variable = self.open_store_variable(name, var[0])
        else:
            variable = self.open_store_variable(name, var)
        return variable

    def open_cfa_variable(self, name: str, var, real_agg_dims):
        """
        Open a CFA Aggregated variable with the correct parameters to create an Xarray ``Variable`` instance.

        :param name:        (str) A named NetCDF4 variable.

        :param var:         (obj) The NetCDF4.Variable object or a tuple with the contents
                            ``(NetCDF4.Variable, cfa)`` where ``cfa`` is a bool that determines
                            if the variable is a CFA or standard variable.

        :param real_agg_dims:       (dict) Named fragment dimensions with their corresponding sizes in 
                                    array space.

        :returns:           An xarray ``Variable`` instance constructed from the attributes provided here, and
                            data provided by a ``FragmentArrayWrapper`` which is indexed by Xarray's ``LazilyIndexedArray`` class.
        """

        ## Array Metadata
        dimensions  = tuple(real_agg_dims.keys())
        ndim        = len(dimensions)
        array_shape = tuple(real_agg_dims.values())

        if hasattr(var, 'units'):
            units = getattr(var, 'units')
        else:
            units = ''

        ## Get non-aggregated attributes.
        attributes = {}
        for k in var.ncattrs():
            if 'aggregated' not in k:
                attributes[k] = var.getncattr(k) 

        ## Array-like object 
        data = indexing.LazilyIndexedArray(
            FragmentArrayWrapper(
                self._decoded_cfa,
                ndim=ndim,
                shape=array_shape,
                units=units,
                dtype=var.dtype,
                cfa_options=self.cfa_options,
            ))
            
        encoding = {}
        if isinstance(var.datatype, netCDF4.EnumType):
            encoding["dtype"] = np.dtype(
                data.dtype,
                metadata={
                    "enum": var.datatype.enum_dict,
                    "enum_name": var.datatype.name,
                },
            )
        else:
            encoding["dtype"] = var.dtype
        _ensure_fill_value_valid(data, attributes)
        # netCDF4 specific encoding; save _FillValue for later
        filters = var.filters()
        if filters is not None:
            encoding.update(filters)
        chunking = var.chunking()
        if chunking is not None:
            if chunking == "contiguous":
                encoding["contiguous"] = True
                encoding["chunksizes"] = None
            else:
                encoding["contiguous"] = False
                encoding["chunksizes"] = tuple(chunking)
                encoding["preferred_chunks"] = dict(zip(var.dimensions, chunking))
        # TODO: figure out how to round-trip "endian-ness" without raising
        # warnings from netCDF4
        # encoding['endian'] = var.endian()
        pop_to(attributes, encoding, "least_significant_digit")
        # save source so __repr__ can detect if it's local or not
        encoding["source"] = self._filename
        encoding["original_shape"] = data.shape

        v = Variable(dimensions, data, attributes, encoding)
        return v

    def _get_xarray_fragment(self, filename, address, dtype, units, shape):
        dsF = xarray.open_dataset(filename)
        fragment = dsF[address]
        assert fragment.shape == shape
        assert fragment.dtype == dtype

        if hasattr(fragment, 'units'):
            assert fragment.units == units
        elif units != None:
            print("Warning: Fragment does not contain units, while units were expected")

        return fragment

    def _perform_decoding(self, location, address, file, cformat, term, substitutions=None):
        aggregated_data = {}

        ndim = location.shape[0]

        chunks = [i.compressed().tolist() for i in location]
        shape = [sum(c) for c in chunks]
        positions = chunk_positions(chunks)
        locations = chunk_locations(chunks)

        if term is not None:
            # --------------------------------------------------------
            # This fragment contains a constant value, not file
            # locations.
            # --------------------------------------------------------
            term = str(term)
            fragment_shape = term.shape
            aggregated_data = {
                frag_loc: {
                    "location": loc,
                    "fill_value": term[frag_loc].item(),
                    "format": "full",
                }
                for frag_loc, loc in zip(positions, locations)
            }
        else:

            extra_dimension = file.ndim > ndim
            if extra_dimension:
                # There is an extra non-fragment dimension
                fragment_shape = file.shape[:-1]
            else:
                fragment_shape = file.shape

            #print(f.shape, a.getValue(), a.dtype)

            if not address.ndim:
                addr = address.getValue()
                adtype = np.array(addr).dtype
                address = np.full(fragment_shape, addr, dtype=adtype)

            if not cformat.ndim:
                # Properly convert into numpy types
                cft = cformat.getValue()
                npdtype = np.array(cft).dtype
                cformat = np.full(fragment_shape, cft, dtype=npdtype)

            if extra_dimension:
                aggregated_data = {
                    frag_loc: {
                        "location": loc,
                        "filename": file[frag_loc].tolist(),
                        "address": address[frag_loc].tolist(),
                        "format": cformat[frag_loc].item(),
                    }
                    for frag_loc, loc in zip(positions, locations)
                }
            else:
                aggregated_data = {
                    frag_loc: {
                        "location": loc,
                        "filename": file[frag_loc],
                        "address": address[frag_loc],
                        "format": cformat[frag_loc],
                    }
                    for frag_loc, loc in zip(positions, locations)
                }

            # Apply string substitutions to the fragment filenames
            if substitutions:
                for value in aggregated_data.values():
                    for base, sub in substitutions.items():
                        value["filename"] = value["filename"].replace(base, sub)

        return fragment_shape, aggregated_data

    def perform_decoding(self):

        try:
            location = self.ds.variables['cfa_location']
            file     = self.ds.variables['cfa_file']
            address  = self.ds.variables['cfa_address']
            cformat  = self.ds.variables['cfa_format']
        except:
            raise ValueError(
                "Unable to locate CFA Decoding instructions"
            )

        fragment_shape, aggregated_data = self._perform_decoding(location, address, file, cformat, term=None, substitutions=xarray_subs)

        self._decoded_cfa = {
            'fragment_shape': fragment_shape,
            'aggregated_data': aggregated_data
        }

    def test_load(self):

        param1 = self.ds.ncattrs()
        param2 = self.ds.variables

        ## CFA Instruction Variables

        # Location is the most complicated to deal with - must be expanded.
        location = self.ds.variables['cfa_location']
        file     = self.ds.variables['cfa_file']
        address  = self.ds.variables['cfa_address']
        cformat  = self.ds.variables['cfa_format']

        ## Aggregated Variables
        #aggregated_vars = {avar: self.ds.variables[avar] for avar in self.ds.dimensions.keys() if hasattr(self.ds.variables[avar], 'aggregated_dimensions')}


        ## Aggregation Dimensions
        # Important aggregation dimensions start with 'f_' - assumption!
        #cfa_dims = {cfd: self.ds.dimensions[cfd] for cfd in self.ds.dimensions.keys() if 'f_' in cfd}
        std_dims = [d for d in self.ds.dimensions.keys() if 'f_' not in d]

        fragment_shape, aggregated_data = self._perform_decoding(location, address, file, cformat, None, substitutions=xarray_subs)

        fragments = []
        # Recheck how cf-python does the decoding.

        concat_dims = [std_dims[i] for i in range(len(fragment_shape)) if fragment_shape[i] > 1]

        for fragment in aggregated_data.keys():
            finfo = aggregated_data[fragment]
            arr_fragment = self._get_xarray_fragment(
                filename=finfo['filename'],
                address=finfo['address'],
                dtype=np.dtype(np.float64), # from aggregated vars
                shape=(2,180,360), # from aggregated vars
                units=None, # from aggregated vars
            )

            # Open all fragments as xarray sections and combine into a single data array
            fragments.append(arr_fragment)

        agg_ds = xarray.combine_nested(fragments, concat_dims)
        
        return None
