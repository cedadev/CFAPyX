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
        self.cdim_attrs    = {}
        self.var_attrs    = {}

        self.var_info     = {}
        self.dim_info     = {}
        self.cdim_arrays  = {}

        self.fragment_space = None

        self.location = None

        self.cdim_sizes = None
        self.cdim_opts = None

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
        # cdim_arrays now sorted to the same order as fragment_space

        location_dims = []

        for attr, value in self.global_attrs.items():
            ds.setncattr(attr, value)

        ds.Conventions = 'CF-1.12'

        for dim, di in self.dim_info.items():
            scalar = ds.createDimension(
                dim,
                di['size']
            )
            if 'attrs' in di:
                for attr, val in di['attrs'].items():
                    scalar.setncattr(attr, val)

        for cdimname, cdimension in self.cdim_arrays.items():

            axis = ds.createDimension(
                cdimname, 
                cdimension.size,
            )

            f_axis = ds.createDimension(
                f'f_{cdimname}',
                self.fragment_space[axis_id],
            )

            location_dims.append(f'f_{cdimname}')

            axis_var = ds.createVariable(
                cdimname,
                cdimension.dtype,
                (cdimname,)
            )
            for k, v in self.cdim_attrs[cdimname].items():
                axis_var.setncattr(k, v)

            axis_var[:] = cdimension

            axes.append(axis)
            axes.append(f_axis)
            axes_vars.append(axis_var)

            axis_id += 1


        for x, opt in enumerate(self.cdim_opts):
            ndims = ds.createDimension(
                f'shape_{x}',
                len(opt),
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

            if not meta['cdims']:
                variable = self._create_scalar_variable(ds, var, meta, self.var_attrs[var])
                variables.append(variable)
            else:

                agg_dims = ' '.join(meta['dims'])

                num = None
                for n, opt in enumerate(self.cdim_opts):
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

        cdimlens = {}
        for d in self.cdim_sizes.keys():
            cdimlens[d] = len(self.cdim_sizes[d])

        for num, cdims in enumerate(self.cdim_opts):
            # opt is a tuple of the cdimensions for this set of instructions.

            largest = 0
            i_dim = ''

            for d in cdims:
                if d not in cdimlens:
                    continue
                if cdimlens[d] > largest:
                    largest = cdimlens[d]
                    i_dim = f'f_{d}'

            # Find the largest of the dimensions
            # Set dim_sizes accordingly
            shape_name    = f'fragment_shape_{num}'

            shapes = []

            for d in cdims:
                if d in self.cdim_sizes:
                    sizes = self.cdim_sizes[d]
                else:
                    sizes = [2] # FIX
                shapes.append(fill_empty(sizes, largest))

            shape = ds.createVariable(
                shape_name,
                int, # Type
                (f'shape_{num}', i_dim)
            )

            shapes = np.array(shapes)
            shapes = np.ma.array(shapes, dtype=int, mask=(shapes==0))
            shape[:,:] = shapes

    def _create_aggregated_variable(self, ds, var, meta, attrs, agg_dims, agg_data):

        var_arr = ds.createVariable(
            var,
            meta['dtype'],
            (),
            fill_value = meta['_FillValue'],
        )

        for k, v in attrs.items():
            try:
                var_arr.setncattr(k, v)
            except Exception as err:
                print(
                    f'Cannot set attribute - {k}: {v} for {var}'
                )
                print(err)

        var_arr.aggregated_dimensions = agg_dims
        var_arr.aggregated_data = agg_data

        return var_arr

    def _create_scalar_variable(self, ds, var, meta, attrs):

        var_arr = ds.createVariable(
            var,
            meta['dtype'],
            meta['dims'],
            fill_value = meta['_FillValue'],
        )

        for k, v in attrs.items():
            try:
                var_arr.setncattr(k, v)
            except Exception as err:
                print(
                    f'Cannot set attribute - {k}: {v} for {var}'
                )
                print(err)

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

        global_attrs, cdim_attrs = {}, {}

        var_attrs = None

        var_info = {}
        dim_info = None

        cdim_arrays = None
        cdim_starts = None
        cdim_sizes  = None

        arranged_files = {}

        longest_filename = ''

        for x, f in enumerate(self.files):

            print(f'[INFO] File {x+1}/{len(self.files)}')
            for file in f:
                if len(file) > len(longest_filename):
                    longest_filename = file

            file = f[0]

            ds = netCDF4.Dataset(file)

            all_dims = ds.dimensions.keys()

            all_vars = ds.variables.keys()

            # Preserve original order wherever possible.

            cdims, dims, vars = [],[],[]

            # Annoying use of for loops because I can't use unordered sets.
            for d in all_dims:
                if d in all_vars:
                    cdims.append(d)
                else:
                    dims.append(d)

            for v in all_vars:
                if v not in all_dims:
                    vars.append(v)

            #cdims = sorted(all_dims & all_vars) # Coordinate variables - Dims that are also vars
            #dims  = sorted(all_dims - all_vars) # Dims that are not also vars
            #vars  = sorted(all_vars - all_dims) # Vars that are not dims.

            print(f'[INFO] Coordinate dimensions: {cdims}')
            print(f'[INFO] Other dimensions: {dims}')
            print(f'[INFO] Variables: {vars}')

            if cdim_arrays is None:
                cdim_arrays = {d: [] for d in cdims}
                cdim_starts = {d: [] for d in cdims}
                cdim_sizes  = {d: [] for d in cdims}

            if var_info != {} and len(set(vars) ^ set(var_info.keys())) != 0:
                raise ValueError(
                    'Differing numbers of variables across the fragment files '
                    'is not currently supported.'
                )

            ## Accumulate global attributes
            ncattrs = {}
            for attr in ds.ncattrs():
                ncattrs[attr] = ds.getncattr(attr)
            global_attrs = self._accumulate_attrs(global_attrs, ncattrs)

            ## Determine scalar dimension info

            if dim_info is None:
                dim_info = {}
                for d in dims:
                    dim_info[d] = {'size': ds.dimensions[d].size}
                    attrs = {}
                    if hasattr(ds.dimensions[d], 'ncattrs'):
                        for attr in ds.dimensions[d].ncattrs():
                            attrs[attr] = ds.dimensions[d].getncattr(attr)
                        dim_info[d]['attrs'] = attrs
            else:
                pdims = set(dim_info.keys())
                if bool(pdims - set(dims)) or bool(set(dims) - pdims):
                    raise ValueError(
                        'Differing dimensions across the fragment files '
                        'is not currently supported.'
                    )

            ## Determine coordinate dimension information
            dsattrs   = {}

            fcoord = []
            for d in cdims:
                array, start, size = self._extract_dimension(ds, d)
                fcoord.append(start.item())

                if start not in cdim_starts[d]:
                    cdim_arrays[d].append(array)
                    cdim_starts[d].append(start)
                    cdim_sizes[d].append(size)

                dsattrs[d] = {}
                for a in ds.variables[d].ncattrs():
                    dsattrs[d][a] = ds.variables[d].getncattr(a)

            arranged_files[tuple(fcoord)] = f

            ## Determine cdimension attributes
            cdim_attrs = self._accumulate_attrs(cdim_attrs, dsattrs)

            if var_attrs is None:
                var_attrs = {v: {} for v in vars}

            ## Accumulate var_info
            vattrs = {}
            for v in vars:

                vattrs[v] = {}
                for a in ds.variables[v].ncattrs():
                    vattrs[a] = ds.variables[v].getncattr(a)

                try:
                    fill = ds[v].getncattr('_FillValue')
                except:
                    fill = None

                vdims = []
                for d in ds[v].dimensions: # Preserving the dimensions per variable
                    if d in cdims:
                        vdims.append(d)

                var_info[v] = {
                    'dtype': np.dtype(ds[v].dtype),
                    'dims' : tuple(ds[v].dimensions),
                    'cdims': vdims,
                    'address': v, # Or match with replacement,
                    '_FillValue': fill,
                }

                var_attrs[v] = self._accumulate_attrs(var_attrs[v], vattrs)

        named_cdims = list(cdim_attrs.keys())

        self.global_attrs = global_attrs
        self.cdim_attrs = cdim_attrs
        self.var_attrs = var_attrs

        self.dim_info = dim_info

        ndimsizes = {}
        for d in cdim_starts.keys():

            # Should be a unique set for each now
            narr = np.array(cdim_starts[d])
            arr  = narr.astype(np.float64)
            sort = np.argsort(arr)

            cdimarr = None
            nds = []
            for s in sort:

                if cdimarr is None:
                    cdimarr = np.array(cdim_arrays[d][s])
                else:
                    cdimarr = np.concatenate((cdimarr, np.array(cdim_arrays[d][s])))

                nds.append(cdim_sizes[d][s])

            cdim_arrays[d] = cdimarr
            ndimsizes[d]   = tuple(np.array(nds))

        cdimopts = []
        for v in var_info.values():
            cds = v['dims'] # Need to pass all dimensions here.
            if cds and sorted(cds) not in cdimopts:
                cdimopts.append(cds)

        self.fragment_space = [len(ndimsizes[d]) for d in cdim_starts.keys()]

        location_space = list(self.fragment_space)

        location_space.append(self.max_files)

        # Create aggregated location
        # Map arranged_files keys to coords using cdim_starts.

        location = np.empty(location_space, dtype=f'<U{len(longest_filename)}')
        for coord in arranged_files.keys():

            new_coord = []
            for x, c in enumerate(coord):
                new_coord.append(
                    cdim_starts[named_cdims[x]].index(c)
                )

            location[tuple(new_coord)] = arranged_files[coord]

        self.var_info   = var_info
        self.cdim_arrays = cdim_arrays
        self.cdim_sizes  = ndimsizes
        self.cdim_opts   = cdimopts

        self.location   = location

    def _accumulate_attrs(self, attrs, ncattrs):

        if not attrs:
            first_time = True

        for attr in ncattrs.keys():
            if attr == '_FillValue':
                continue

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

        dimension_array = np.array(list(ds[d]), dtype=ds[d].dtype)
        start = dimension_array[0]

        return dimension_array, start, len(dimension_array)
