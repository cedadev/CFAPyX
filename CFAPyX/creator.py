__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import netCDF4
import numpy as np

CONCAT_MSG = 'See individual datasets for more information.'

class CFANetCDF:
    def __init__(self, files, concat_msg=CONCAT_MSG):


        if isinstance(files, str):
            raise NotImplementedError(
                'Matching files from a pattern not yet implemented'
            )
        else:
            self.files = self._filter_files(files)

        self.global_attrs = {}
        self.dim_attrs    = {}
        self.var_attrs    = {}

        self.var_info     = {}
        self.dim_arrays = {}

        self.fragment_space = None

        self.location = None

        self.dim_sizes = None
        self.dim_opts = None

        self.concat_msg = concat_msg

        self.max_files = 1

    def create(
            self, 
            updates : dict = None,
            removals: list = None
        ) -> None:

        updates  = updates or {}
        removals = removals or []

        self._obtain_data()

    def write(
            self, 
            outfile: str
        ) -> None:

        ds = netCDF4.Dataset(outfile, mode='w', format='NETCDF4', maskandcale=True)

        axes, axes_vars = [], []
        axis_id = 0
        # dim_arrays now sorted to the same order as fragment_space

        location_dims = []

        for attr, value in self.global_attrs.items():
            setattr(ds, attr, value)

        ds.Conventions = 'CF-1.12'

        for dimname, dimension in self.dim_arrays.items():

            axis = ds.createDimension(
                dimname, 
                dimension.size,
            )

            f_axis = ds.createDimension(
                f'f_{dimname}',
                self.fragment_space[axis_id],
            )

            location_dims.append(f'f_{dimname}')

            axis_var = ds.createVariable(
                dimname,
                dimension.dtype,
                (dimname,)
            )
            for k, v in self.dim_attrs[dimname].items():
                setattr(axis_var, k, v)

            axis_var[:] = dimension

            axes.append(axis)
            axes.append(f_axis)
            axes_vars.append(axis_var)

            axis_id += 1

        ndims = ds.createDimension(
            'j',
            len(self.fragment_space),
        )

        nfiles = ds.createDimension(
            'versions',
            self.max_files,
        )

        location_dims.append('versions')

        var_addresses = {v['address']: v['address'] for v in self.var_info.values()}

        self._create_fragment_array_variables(ds, var_addresses, location_dims, self.location)

        variables = []

        for var, meta in self.var_info.items():

            agg_dims = ' '.join(meta['dims'])

            num = None
            for n, opt in enumerate(self.dim_opts):
                if opt == meta['dims']:
                    num = n

            if num is None:
                raise ValueError(
                    'Dimension mismatch issue.'
                )

            agg_data = ' '.join([
                'location: fragment_location',
                f'address: fragment_address_{var}',
                f'shape: fragment_shape_{num}'
            ])

            variable = self._create_aggregated_variable(ds, var, meta, self.var_attrs[var], agg_dims, agg_data)
            variables.append(variable)

        ds.close()

    def _create_fragment_array_variables(self, ds, var_addresses, location_dims, location_data):

        # Create location
        # Create addresses for all variables
        # Create shapes

        location = ds.createVariable(
            'fragment_location',
            str,
            location_dims
        )

        location[(slice(0,None) for i in location_dims)] = location_data

        addrs = []
        for source, frag in var_addresses.items():
            addr = ds.createVariable(
                f'fragment_address_{source}',
                str,
                (),
            )
            addr[:] = np.array(frag, dtype=str)
            addrs.append(addr)

        self._create_fragment_array_shapes(ds)

    def _create_fragment_array_shapes(self, ds):
        # From dim opts, construct the set of fragment array variables for each option

        def fill_empty(array, size):
            array = list(array)
            init_length = int(len(array))
            for x in range(size - init_length):
                array.append(0)
            return tuple(array)

        dimlens = {}
        for d in self.dim_sizes.keys():
            dimlens[d] = len(self.dim_sizes[d])

        for num, dims in enumerate(self.dim_opts):
            # opt is a tuple of the dimensions for this set of instructions.

            largest = 0
            i_dim = ''

            for d in dims:
                if dimlens[d] > largest:
                    largest = dimlens[d]
                    i_dim = f'f_{d}'

            # Find the largest of the dimensions
            # Set dim_sizes accordingly
            shape_name    = f'fragment_shape_{num}'

            shapes = []

            for d in self.dim_sizes.keys():
                if d in dims:
                    shapes.append(fill_empty(self.dim_sizes[d], largest))

            shape = ds.createVariable(
                shape_name,
                int, # Type
                ('j', i_dim)
            )

            shapes = np.array(shapes)
            shapes = np.ma.array(shapes, dtype=int, mask=(shapes==0))
            shape[:,:] = shapes

    def _create_aggregated_variable(self, ds, var, meta, attrs, agg_dims, agg_data):

        var_arr = ds.createVariable(
            var,
            meta['dtype'],
            (),
        )

        for k, v in attrs.items():
            setattr(var_arr, k, v)

        var_arr.aggregated_dimensions = agg_dims
        var_arr.aggregated_data = agg_data

        return var_arr

    def _filter_files(self, files):

        filtered = []
        trailing_file = False

        max_files = 0
        for f in files:
            if isinstance(f, tuple):
                trailing_file = True
                if max_files < len(f):
                    max_files = len(f)

        for f in files:
            if trailing_file:
                fileopts = [''] * max_files
                if isinstance(f, tuple):
                    for x, c in enumerate(f):
                        fileopts[x] = c
                else:
                    fileopts[0] = f
                filtered.append(tuple(fileopts))
            else:
                filtered.append((f,))

        self.max_files = max_files

        return filtered

    def _obtain_data(self):

        global_attrs, dim_attrs, var_attrs = {}, {}, {}

        var_info = {}

        dim_arrays = None
        dim_starts = None
        dim_sizes  = None

        arranged_files = {}

        longest_filename = ''

        for f in self.files:

            for file in f:
                if len(file) > len(longest_filename):
                    longest_filename = file

            file = f[0]

            ds = netCDF4.Dataset(file)

            dims = sorted(list(ds.dimensions.keys()))
            vars = set(ds.variables.keys()) ^ set(dims)

            if dim_arrays is None:
                dim_arrays = {d: [] for d in dims}
                dim_starts = {d: [] for d in dims}
                dim_sizes  = {d: [] for d in dims}

            if var_info != {} and len(vars ^ set(var_info.keys())) != 0:
                raise ValueError(
                    'Differing numbers of variables across the fragment files '
                    'is not currently supported.'
                )

            ## Accumulate global attributes
            ncattrs = {}
            for attr in ds.ncattrs():
                ncattrs[attr] = ds.getncattr(attr)
            global_attrs = self._accumulate_attrs(global_attrs, ncattrs)

            ## Determine dimension information
            dsattrs   = {}

            fcoord = []
            for d in dims:
                array, start, size = self._extract_dimension(ds, d)
                fcoord.append(start.item())

                if start not in dim_starts[d]:
                    dim_arrays[d].append(array)
                    dim_starts[d].append(start)
                    dim_sizes[d].append(size)

                dsattrs[d] = {}
                for a in ds.variables[d].ncattrs():
                    dsattrs[d][a] = ds.variables[d].getncattr(a)

            arranged_files[tuple(fcoord)] = f

            ## Determine dimension attributes
            dim_attrs = self._accumulate_attrs(dim_attrs, dsattrs)

            ## Accumulate var_info
            vattrs = {}
            for v in vars:
                vattrs[v] = {}
                for a in ds.variables[v].ncattrs():
                    vattrs[a] = ds.variables[v].getncattr(a)

                var_info[v] = {
                    'dtype': np.dtype(ds[v].dtype),
                    'dims' : sorted(tuple(ds[v].dimensions)),
                    'address': v, # Or match with replacement
                }

            var_attrs = self._accumulate_attrs(var_attrs, vattrs)

        named_dims = list(dim_attrs.keys())

        self.global_attrs = global_attrs
        self.dim_attrs = dim_attrs
        self.var_attrs = var_attrs

        ndimarrays = {}
        ndimsizes = {}
        locations = {}
        for d in dim_starts.keys():

            # Should be a unique set for each now
            narr = np.array(dim_starts[d])
            arr  = narr.astype(np.float64)
            sort = np.argsort(arr)

            ndimarrays[d]  = np.array(dim_arrays[d])[sort]
            ndimsizes[d]   = tuple(np.array(dim_sizes[d])[sort])

        dim_arrays = {}
        for d in ndimarrays.keys():
            dim_arrays[d] = np.concatenate(ndimarrays[d])

        dimopts = []
        for v in var_info.values():
            if sorted(v['dims']) not in dimopts:
                dimopts.append(sorted(v['dims']))

        self.fragment_space = [len(ndimsizes[d]) for d in dim_starts.keys()]

        location_space = list(self.fragment_space)

        location_space.append(self.max_files)

        # Create aggregated location
        # Map arranged_files keys to coords using dim_starts.

        location = np.empty(location_space, dtype=f'<U{len(longest_filename)}')
        for coord in arranged_files.keys():

            new_coord = []
            for x, c in enumerate(coord):
                new_coord.append(
                    dim_starts[named_dims[x]].index(c)
                )

            location[tuple(new_coord)] = arranged_files[coord]

        self.var_info   = var_info
        self.dim_arrays = dim_arrays
        self.dim_sizes  = ndimsizes
        self.dim_opts   = dimopts

        self.location   = location

    def _accumulate_attrs(self, attrs, ncattrs):

        if not attrs:
            first_time = True

        for attr in ncattrs.keys():
            if attr not in attrs:
                if first_time:
                    attrs[attr] = ncattrs[attr]
                else:
                    print(f'AttributeWarning: Attribute "{attr}" not present in all files')
                    attrs[attr] = self.concat_msg
            else:
                if attrs[attr] != ncattrs[attr]:
                    attrs[attr] = self.concat_msg
                else:
                    attrs[attr] = ncattrs[attr]
        return attrs

    def _extract_dimension(self, ds, d: str):
        start = ds[d][0].data
        end   = ds[d][-1].data

        dimension_array = np.array(list(ds[d]), dtype=ds[d].dtype)

        return dimension_array, start, len(dimension_array)
