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

from CFAPyX.utils import _ensure_fill_value_valid
from CFAPyX.fragmentarray import FragmentArrayWrapper
from CFAPyX.decoder import chunk_locations, chunk_positions

xarray_subs = {
    'file:///':'/'
}

class CFADataStore(NetCDF4DataStore):

    def get_variables(self):
        """
        Optional override for get_variables method - may not be needed
        """

        xarray_vars = {}

        standardised_terms = (
            "cfa_location",
            "cfa_file",
            "cfa_address",
            "cfa_format"
        )

        for avar in self.ds.variables.keys():
            cfa = False
            if hasattr(self.ds.variables[avar], 'aggregated_dimensions'):
                cfa = True

            # Ignore CFA standardised terms here
            if avar not in standardised_terms:
                xarray_vars[avar] = (self.ds.variables[avar], cfa)

        if not hasattr(self, '_decoded_cfa'):
            self.perform_decoding()

        return FrozenDict(
            (k, self.open_variable(k, v)) for k, v in xarray_vars.items()
        )

    def get_attrs(self):
        """
        Optional override for get_attrs - may be required for aggregated variables.
        """
        return FrozenDict((k, self.ds.getncattr(k)) for k in self.ds.ncattrs())

    def open_variable(self, name: str, var):
        if type(var) == tuple:
            if var[1]:
                return self.open_cfa_variable(name, var[0])
            else:
                return self.open_store_variable(name, var[0])
        else:
            return self.open_store_variable(name, var)

    def open_cfa_variable(self, name: str, var):

        dimensions = var.dimensions
        attributes = {k: var.getncattr(k) for k in var.ncattrs()}
        # For CFA-type variables create a new DataStore instance for each fragment - that knows how to load a subsection only.

        # Now we've already loaded some elements in _decoded_cfa
        # Assume aggregated_instructions are loaded into _decoded_cfa so we just need to build the 
        # array inside the Lazy Indexer somehow:
        # - Open fragment using netcdf4 library
        # - Select slice based on instructions
        # - Return as data here.

        # Note: Cannot combine as variables, must be combined into one LazilyIndexedArray at the point of
        # creating a new Variable.

        # What to do with the array?
        data = indexing.LazilyIndexedArray(
            FragmentArrayWrapper(
                self._decoded_cfa
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

        return Variable(dimensions, data, attributes, encoding)

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
        location = self.ds.variables['cfa_location']
        file     = self.ds.variables['cfa_file']
        address  = self.ds.variables['cfa_address']
        cformat  = self.ds.variables['cfa_format']

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
                dtype=np.float64, # from aggregated vars
                shape=(2,180,360), # from aggregated vars
                units=None, # from aggregated vars
            )

            # Open all fragments as xarray sections and combine into a single data array
            fragments.append(arr_fragment)

        agg_ds = xarray.combine_nested(fragments, concat_dims)
        
        return None
