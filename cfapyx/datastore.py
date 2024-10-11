__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from xarray.backends import ( 
    NetCDF4DataStore
)

from xarray.core.utils import FrozenDict
from xarray.core import indexing
from xarray.coding.variables import pop_to
from xarray.core.variable import Variable

import netCDF4
import numpy as np
import re

from cfapyx.wrappers import FragmentArrayWrapper
from cfapyx.decoder import get_fragment_positions, get_fragment_extents
from cfapyx.group import CFAGroupWrapper

import logging

logger = logging.getLogger(__name__)


xarray_subs = {
    'file:///':'/'
}

class CFADataStore(NetCDF4DataStore):

    """
    DataStore container for the CFA-netCDF loaded file. Contains all unpacking routines 
    directly related to the specific variables and attributes. The ``NetCDF4Datastore``
    Xarray class from which this class inherits, has an ``__init__`` method which 
    cannot easily be overriden, so properties are used instead for specific variables 
    that may be un-set at time of use.
    """

    @property
    def chunks(self):
        if hasattr(self,'_cfa_chunks'):
            return self._cfa_chunks
        return None
    
    @chunks.setter
    def chunks(self, value):
        self._cfa_chunks = value

    @property
    def cfa_options(self):
        """
        Property of the datastore that relates private option variables to the standard 
        ``cfa_options`` parameter.
        """

        return {
            'substitutions': self._substitutions,
            'decode_cfa': self._decode_cfa,
            'chunks': self.chunks,
            'chunk_limits': self._chunk_limits,
            'use_active': self.use_active
        }

    @cfa_options.setter
    def cfa_options(self, value):
        self._set_cfa_options(**value)

    def _set_cfa_options(
            self, 
            substitutions=None, 
            decode_cfa=True,
            chunks={},
            chunk_limits=True,
            use_active=False,
        ):
        """
        Method to set cfa options.

        :param substitutions:   (dict) Set of provided substitutions to Xarray, 
                                following the CFA conventions on substitutions.

        :param decode_cfa:      (bool) Optional setting to disable CFA decoding 
                                in some cases, default is True.

        :param use_active:      (bool) Enable for use with XarrayActive.

        :param chunks:          (dict) Not implemented in 2024.9.0

        :param chunk_limits:    (dict) Not implemented in 2024.9.0
        """

        self.chunks = chunks
        self._substitutions = substitutions
        self._decode_cfa    = decode_cfa
        self._chunk_limits  = chunk_limits
        self.use_active     = use_active

    def _acquire(self, needs_lock=True):
        """
        Fetch the global or group dataset from the Datastore Caching Manager (NetCDF4)
        """
        with self._manager.acquire_context(needs_lock) as root:
            ds = CFAGroupWrapper.open(root, self._group, self._mode)

        self.conventions = ds.Conventions

        return ds

    def _decode_feature_data(self, feature_data, readd={}):
        """
        Decode the value of an object which is expected to be of the form of a 
        ``feature: variable`` blank-separated element list.
        """
        parts = re.split(': | ',feature_data)

        # Anything that uses a ':' needs to be readded after the previous step.
        for k, v in readd:
            for p in parts:
                p.replace(k,v)

        return {k: v for k, v in zip(parts[0::2], parts[1::2])}

    def _check_applied_conventions(self, agg_data):
        """
        Check that the aggregated data complies with the conventions specified in the
        CFA-netCDF file
        """

        required = ('shape', 'location', 'address')
        if 'CFA-0.6.2' in self.conventions.split(' '):
            required = ('location', 'file', 'format')

        for feature in required:
            if feature not in agg_data:
                raise ValueError(
                    f'CFA-netCDF file is not compliant with {self.conventions} '
                    f'Required aggregated data features: "{required}", '
                    f'Received "{tuple(agg_data.keys())}"'
                )

    def _perform_decoding(
            self, 
            shape, 
            address, 
            location, 
            array_shape,
            value=None, 
            cformat='', 
            substitutions=None):
        """
        Private method for performing the decoding of the standard ``fragment array 
        variables``. Any convention version-specific adjustments should be made prior 
        to decoding with this function, namely in the public method of the same name.

        :param shape:       (obj) The integer-valued ``shape`` fragment array variable 
                            defines the shape of each fragment's data in its canonical 
                            form. CF-1.12 section 2.8.1

        :param address:     (obj) The ``address`` fragment array variable, that may 
                            have any data type, defines how to find each fragment 
                            within its fragment dataset. CF-1.12 section 2.8.1

        :param location:    (obj) The string-valued ``location`` fragment array 
                            variable defines the locations of fragment datasets using 
                            Uniform Resource Identifiers (URIs). CF-1.12 section 2.8.1

        :param value:       (obj) *Optional* unique data value to fill a fragment array 
                            where the data values within the fragment are all the same.

        :param cformat:     (str) *Optional* ``format`` argument if provided by the 
                            CFA-netCDF or cfa-options parameters. CFA-0.6.2

        :param substitutions:   (dict) Set of substitutions to apply in the form 'base':'sub'

        :returns:       (fragment_info) A dictionary of fragment metadata where each 
                        key is the coordinates of a fragment in index space and the 
                        value is a dictionary of the attributes specific to that 
                        fragment.

        """
        
        fragment_info = {}

        # Extract non-padded fragment sizes per dimension.
        fragment_size_per_dim = [i.compressed().tolist() for i in shape]

        # Derive the total shape of the fragment array in all fragmented dimensions.
        fragment_space    = [len(fsize) for fsize in fragment_size_per_dim]

        # Obtain the positions of each fragment in index space.
        fragment_positions = get_fragment_positions(fragment_size_per_dim)

        global_extent, extent, shapes = get_fragment_extents(
            fragment_size_per_dim, 
            array_shape
        )

        if value is not None:
            # --------------------------------------------------------
            # This fragment contains a constant value, not file
            # locations.
            # --------------------------------------------------------
            fragment_space = value.shape
            fragment_info = {
                frag_pos: {
                    "shape": shapes[frag_pos],
                    "fill_value": value[frag_pos].item(),
                    "global_extent": global_extent[frag_pos],
                    "extent": extent[frag_pos],
                    "format": "full",
                }
                for frag_pos in fragment_positions
            }

            return fragment_info, fragment_space

        if not address.ndim: # Scalar address
            addr    = address.getValue()
            adtype  = np.array(addr).dtype
            address = np.full(fragment_space, addr, dtype=adtype)

        if cformat != '':
            if not cformat.ndim:
                cft = cformat.getValue()
                npdtype = np.array(cft).dtype
                cformat = np.full(fragment_space, cft, dtype=npdtype)

        for frag_pos in fragment_positions:

            fragment_info[frag_pos] = {
                "shape"    : shapes[frag_pos],
                "location" : location[frag_pos],
                "address"  : address[frag_pos],
                "extent"   : extent[frag_pos],
                "global_extent": global_extent[frag_pos]
            }
            if hasattr(cformat, 'shape'):
                fragment_info[frag_pos]["format"] = cformat[frag_pos]

        # Apply string substitutions to the fragment filenames
        if substitutions:
            for value in fragment_info.values():
                for base, sub in substitutions.items():
                    if isinstance(value['location'], str):
                        value["location"] = value["location"].replace(base, sub)
                    else:
                        for v in value["location"]:
                            v = v.replace(base, sub)

        return fragment_info, fragment_space

    # Public class methods

    def perform_decoding(self, array_shape, agg_data):
        """
        Public method ``perform_decoding`` involves extracting the aggregated 
        information parameters and assembling the required information for actual 
        decoding.
        """

        # If not raised an error in checking, we can continue.
        self._check_applied_conventions(agg_data)

        cformat = ''
        value   = None
        try:
            if 'CFA-0.6.2' in self.conventions:
                shape        = self.ds.variables[agg_data['location']]
                location     = self.ds.variables[agg_data['file']]
                cformat      = self.ds.variables[agg_data['format']]
            else: # Default to CF-1.12
                shape        = self.ds.variables[agg_data['shape']]
                location     = self.ds.variables[agg_data['location']]
                if 'value' in agg_data:
                    value    = self.ds.variables[agg_data['value']]

            address = self.ds.variables[agg_data['address']]
        except Exception as err:
            raise ValueError(
                'One or more aggregated data features specified could not be '
                'found in the data: '
                f'"{tuple(agg_data.keys())}"'
                f' - original error: {err}'
            )

        subs = {}
        if hasattr(location, 'substitutions'):
            subs = location.substitutions.replace('https://', 'https@//')
            subs = self._decode_feature_data(subs, readd={'https://':'https@//'})

        return self._perform_decoding(shape, address, location, array_shape,
                                      cformat=cformat, value=value, 
                                      substitutions = xarray_subs | subs) 
        # Combine substitutions with known defaults for using in xarray.

    def get_variables(self):
        """
        Fetch the netCDF4.Dataset variables and perform some CFA decoding if 
        necessary.

        ``ds`` is now a ``GroupedDatasetWrapper`` object from ``CFAPyX.group`` which 
        has flattened the group structure and allows fetching of variables and 
        attributes from the whole group tree from which a specific group may inherit.

        :returns:       A ``FrozenDict`` Xarray object of the names of all variables, 
                        and methods to fetch those variables, depending on if those 
                        variables are standard NetCDF4 or CFA Aggregated variables.
        """

        if not self._decode_cfa:
            return FrozenDict(
                (k, self.open_variable(k, v)) for k, v in self.ds.variables.items()
            )

        # Determine CFA-aggregated variables
        all_vars, real_vars = {}, {}

        fragment_array_vars = []

        ## Ignore variables in the set of standardised terms.
        for avar in self.ds.variables.keys():
            cfa = False
            ## CF-Compliant method of identifying aggregated variables.
            if hasattr(self.ds.variables[avar], 'aggregated_dimensions'):
                cfa = True

                agg_data = self.ds.variables[avar].aggregated_data.split(' ')

                for vname in agg_data:
                    fragment_array_vars += re.split(': | ',vname)
                
            all_vars[avar] = (self.ds.variables[avar], cfa)

        # Ignore fragment array variables at this stage of decoding.
        for var in all_vars.keys():
            if var not in fragment_array_vars:
                real_vars[var] = all_vars[var]


        return FrozenDict(
            (k, self.open_variable(k, v)) for k, v in real_vars.items()
        )

    def get_attrs(self):
        """
        Produce the FrozenDict of attributes from the ``NetCDF4.Dataset`` or 
        ``CFAGroupWrapper`` in the case of using a group or nested group tree.
        """
        return FrozenDict((k, self.ds.getncattr(k)) for k in self.ds.ncattrs())

    def open_variable(self, name: str, var):
        """
        Open a CFA-netCDF variable as either a standard NetCDF4 Datastore variable 
        or as a CFA aggregated variable which requires additional decoding.

        :param name:    (str) A named NetCDF4 variable.

        :param var:     (obj) The NetCDF4.Variable object or a tuple with the contents 
                        ``(NetCDF4.Variable, cfa)`` where ``cfa`` is a bool that 
                        determines if the variable is a CFA or standard variable.

        :returns:       The variable object opened as either a standard store variable 
                        or CFA aggregated variable.
        """
        if type(var) == tuple:
            if var[1] and self._decode_cfa:
                variable = self.open_cfa_variable(name, var[0])
            else:
                variable = self.open_store_variable(name, var[0])
        else:
            variable = self.open_store_variable(name, var)
        return variable

    def open_cfa_variable(self, name: str, var):
        """
        Open a CFA Aggregated variable with the correct parameters to create an 
        Xarray ``Variable`` instance.

        :param name:        (str) A named NetCDF4 variable.

        :param var:         (obj) The NetCDF4.Variable object or a tuple with the 
                            contents ``(NetCDF4.Variable, cfa)`` where ``cfa`` is 
                            a bool that determines if the variable is a CFA or 
                            standard variable.

        :returns:           An xarray ``Variable`` instance constructed from the 
                            attributes provided here, and data provided by a 
                            ``FragmentArrayWrapper`` which is indexed by Xarray's 
                            ``LazilyIndexedArray`` class.
        """

        real_dims = {
            d: self.ds.dimensions[d].size for d in var.aggregated_dimensions.split(' ')
        }
        agg_data  = self._decode_feature_data(var.aggregated_data)

        ## Array Metadata
        dimensions  = tuple(real_dims.keys())
        array_shape = tuple(real_dims.values())

        fragment_info, fragment_space = self.perform_decoding(array_shape, agg_data)

        units = ''
        if hasattr(var, 'units'):
            units = getattr(var, 'units')
        if hasattr(var, 'aggregated_units'):
            units = getattr(var, 'aggregated_units')

        ## Get non-aggregated attributes.
        attributes = {}
        for k in var.ncattrs():
            if 'aggregated' not in k:
                attributes[k] = var.getncattr(k) 

        ## Array-like object 
        data = indexing.LazilyIndexedArray(
            FragmentArrayWrapper(
                fragment_info,
                fragment_space,
                shape=array_shape,
                units=units,
                dtype=var.dtype,
                cfa_options=self.cfa_options,
                named_dims=dimensions,
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

        if data.dtype.kind == "S" and "_FillValue" in attributes:
            attributes["_FillValue"] = np.bytes_(attributes["_FillValue"])

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
    
