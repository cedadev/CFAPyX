__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import netCDF4
import numpy as np
import logging
import glob

from collections import OrderedDict

logger = logging.getLogger(__name__)

CONCAT_MSG = 'See individual datasets for more information.'

class CFACreateMixin:
    """
    Mixin class for ``Create`` methods for a CFA-netCDF dataset.
    """

    def _first_pass(self, agg_dims: list = None) -> tuple:
        """
        Perform a first pass across all provided files. Extracts the global
        attributes and information on all variables and dimensions into 
        separate python dictionaries. Also collects the set of files arranged
        by aggregated dimension coordinates, to be used later in constructing
        the CFA ``fragment_location`` properties.
        """

        logger.info('Performing first pass on the set of files.')

        arranged_files = {}
        var_info = None
        dim_info = None
        global_attrs = None

        ## First Pass - Determine dimensions
        for x, file in enumerate(self.files):
            logger.info(f'First pass: File {x+1}/{len(self.files)}')

            ds = self._call_file(file)

            if len(file) == 1:
                file = file[0]
            
            all_dims = ds.dimensions.keys()
            all_vars = ds.variables.keys()

            coord_variables = []
            pure_dimensions = []
            variables       = []

            ## Sort dimension/variable types - switch to dict with types?
            for d in all_dims:
                if d in all_vars:
                    coord_variables.append(d)
                else:
                    pure_dimensions.append(d)

            for v in all_vars:
                if v not in all_dims:
                    variables.append(v)

            if not dim_info:
                dim_info = {d: {} for d in all_dims}
            if not var_info:
                var_info = {v: {} for v in variables}
                    
            logger.info(f'Coordinate variables: {coord_variables}')
            logger.info(f'Pure dimensions: {pure_dimensions}')
            logger.info(f'Variables: {variables}')

            if var_info:
                if len(set(variables) ^ set(var_info.keys())) != 0:
                    raise ValueError(
                        'Differing numbers of variables across the fragment files '
                        'is not currently supported.'
                    )

            ## Accumulate global attributes
            ncattrs = {}
            for attr in ds.ncattrs():
                ncattrs[attr] = ds.getncattr(attr)
            global_attrs = self._accumulate_attrs(global_attrs, ncattrs)

            ## Accumulate dimension info
            fcoord = []
            first_time = (x == 0)
            for d in all_dims:

                if dim_info[d] == {} and not first_time:
                    raise ValueError(
                        f'Files contain differing numbers of dimensions. "{d}"'
                        'appears to not be present in all files.'
                    )
                
                new_info, arr_components = self._collect_dim_info(
                    ds, d, pure_dimensions, coord_variables, 
                    agg_dims=agg_dims, first_time=first_time)
                
                dim_info = self._update_info(ds.dimensions[d], dim_info, new_info)

                if arr_components is not None:
                    if first_time:
                        for attr in arr_components.keys():
                            dim_info[d][attr] = [arr_components[attr]]
                    else:
                        if arr_components['starts'] not in dim_info[d]['starts']:
                            dim_info[d]['starts'] += [arr_components['starts']]
                            dim_info[d]['sizes']  += [arr_components['sizes']]
                            dim_info[d]['arrays'] += [arr_components['arrays']]

                    fcoord.append(arr_components['starts'].item())

            ## Accumulate var_info
            for v in variables:

                try:
                    fill = ds[v].getncattr('_FillValue')
                except:
                    fill = None

                vdims = []
                for d in ds[v].dimensions: # Preserving the dimensions per variable
                    if d in coord_variables:
                        vdims.append(d)

                new_info = {
                    'dtype': np.dtype(ds[v].dtype),
                    'dims' : tuple(ds[v].dimensions),
                    'cdims': vdims,
                    'address': v, # Or match with replacement,
                    '_FillValue': fill,
                }

                var_info = self._update_info(ds.variables[v], var_info, new_info)

            arranged_files[tuple(fcoord)] = file

        return arranged_files, global_attrs, var_info, dim_info

    def _second_pass(
            self,
            var_info : dict,
            non_aggregated : list
        ) -> dict:

        """
        Second pass through a subset of the files (2) to collect non-aggregated variables
        which will be stored in the CFA file.
        """

        logger.info('Performing a second pass on a subset of files.')
        
        second_set = self.files[:2]
        for x, file in enumerate(second_set):
            logger.info(f'Second pass: File {x+1}/{len(self.files)}')

            ds = self._call_file(file) # Ideally don't want to do this twice.

            for v in non_aggregated:
                new_values = np.array(ds.variables[v])

                if 'data' not in var_info[v]:
                    var_info[v]['data'] = new_values
                else:
                    if not np.array_equal(new_values, var_info[v]['data']):
                        raise ValueError(
                            f'Non-aggregated variable "{v}" differs between sample files.'
                        )
        return var_info

    def _collect_dim_info(
            self,
            ds,
            d : str,
            pure_dimensions : list,
            coord_variables : list,
            agg_dims : list = None,
            first_time : bool = False,
        ):

        """
        Collect new info about each dimension. The collected attributes
        depend mostly on if the dimension is ``pure`` (i.e not a coordinate
        variable) or if it is a coordinate variable. Aggregated dimensions require
        collection of all array sequences that have a different ``start`` value. 
        If the aggregation dimensions are known, we do not have to collect arrays
        from each file from non-aggregated dimensions."""

        arr_components = None

        if first_time:
            agg_dims = coord_variables
        else:
            if agg_dims is None or agg_dims == []:
                agg_dims = coord_variables

        if d in pure_dimensions:

            new_info = {
                'size': ds.dimensions[d].size,
                'type':'pure',
                'f_size': 1,
            }
        else:
            new_info = {
                'size': None,
                'type': 'coord',
                'dtype': ds[d].dtype,
                'f_size': None,
            }
            
            if d in agg_dims:

                array = np.array(list(ds[d]), dtype=ds[d].dtype)
                start = array[0]
                size  = len(array)

                arr_components = {
                    'sizes': size,
                    'starts': start,
                    'arrays': array,
                }

        return new_info, arr_components

    def _update_info(
            self,
            ncattr_obj,
            info : dict,
            new_info: dict,
        ) -> dict:

        """
        Update the information for a variable/dimension based on the
        current dataset. Certain properties are collected in lists while
        others are explicitly defined and should be equal across all files.
        Others still may differ across files, in which case the ``concat_msg``
        is applied which usually indicates to inspect individual files for
        the correct value.
        """
        
        id = ncattr_obj.name
        logger.debug(f'Concatenating information for {id}')

        attrs = {}
        if hasattr(ncattr_obj, 'ncattrs'):
            for attr in ncattr_obj.ncattrs():
                attrs[attr] = ncattr_obj.getncattr(attr)

        if info[id] != {}:
            info[id]['attrs'] = self._accumulate_attrs(info[id]['attrs'], attrs)

            for attr, value in new_info.items():
                if value != info[id][attr]:
                    raise ValueError(
                        f'Info not matching between files for "{id}": "{attr}"'
                    )
        else:
            info[id] = new_info
            info[id]['attrs'] = attrs

        return info

    def _arrange_dimensions(
            self,
            dim_info : dict,
            agg_dims : list = None
        ) -> dict:

        """
        Arrange the aggregation dimensions by ordering the 
        start values collected from each file. Dimension arrays are 
        aggregated to a single array once properly ordered, and the sizes
        fragments in each dimension are recorded in the ``dim_info``.
        """

        logger.info('Performing aggregated dimension sorting')

        ## Arrange Aggregation Dimensions
        aggregation_dims = []
        for cd, info in dim_info.items():

            if 'starts' not in info:
                continue

            starts = info['starts'] # Needed later
            arrays = info.pop('arrays')
            sizes  = info['sizes']

            dim_info[cd]['f_size'] = len(starts)

            if len(starts) == 1:
                cdimarr = arrays[0]
                ndimsizes = (sizes[0],)

            else:

                ## Still here means the dimension requires aggregation.
                aggregation_dims.append(cd)

                narr = np.array(starts)
                arr  = narr.astype(np.float64)
                sort = np.argsort(arr)

                cdimarr = None
                nds = []
                for s in sort:

                    if cdimarr is None:
                        cdimarr = np.array(arrays[s])
                    else:
                        cdimarr = np.concatenate((cdimarr, np.array(arrays[s])))

                    nds.append(sizes[s])

                ndimsizes   = tuple(nds) # Removed np.array here

            info['size'] = cdimarr.size
            info['array'] = cdimarr
            info['sizes'] = ndimsizes

        if agg_dims is not None:
            if len(agg_dims) != len(aggregation_dims):
                raise ValueError(
                    'Found fewer aggregation dims than user provided value.'
                    f'User defined: ({list(agg_dims)})'
                    f'Derived: ({list(aggregation_dims)})'
                )

        return dim_info, aggregation_dims

    def _assemble_location(
            self, 
            arranged_files : dict, 
            dim_info : dict
        ) -> dict:

        """
        Assemble the base CFA ``fragment_location`` from which all the 
        locations for different variables are derived. Locations are defined
        by the number of dimensions, and follow the same pattern for definition
        as the ``fragment_shapes``. The combinations of dimensions that
        require their own ``location`` and ``shape`` are recorded in ``cdim_opts``.
        """

        logger.debug('Assembling the location variable')

        # Define the location space
        location_space      = tuple(i for i in self.fragment_space if i > 1)
        if self.max_files > 1:
            location_space = location_space + (self.max_files,)

        # Assemble the set of named dims
        named_cdims = [k for k, v in dim_info.items() if v['type'] == 'coord']

        # Initialise empty location container
        location = np.empty(location_space, dtype=f'<U{len(self.longest_filename)}')

        # Map collected coords to proper place for location.
        for coord in arranged_files.keys():

            new_coord = []
            for x, c in enumerate(coord):
                if self.fragment_space[x] > 1:
                    new_coord.append(
                        dim_info[named_cdims[x]]['starts'].index(c)
                    )

            location[tuple(new_coord)] = arranged_files[coord]
        
        return location

    def _apply_agg_dims(
            self,
            var_info,
            agg_dims
        ):
        for var, meta in var_info.items():

            aggs = []

            if 'cdims' not in meta:
                continue

            for cd in meta['cdims']:
                if cd in agg_dims:
                    aggs.append(cd)

            if aggs:
                var_info[var]['adims'] = tuple(aggs)
    
        return var_info

    def _determine_non_aggregated(
            self, 
            var_info : dict, 
            agg_dims : list
        ) -> list:

        """
        Determine non-aggregated variables present in the fragment files.
        Non-aggregated variables are equivalent to the ``identical variables``
        from kerchunk jargon. If the non-aggregated variables are later found
        to vary across the fragment files, an error will be raised.
        """

        non_aggregated = []
        for var, info in var_info.items():
            if not (set(info['cdims']) & set(agg_dims)):
                logger.info('Second pass required to extract non-aggregated variable values')
                non_aggregated.append(var)

        logger.debug(f'Non-aggregated variables: {tuple(non_aggregated)}')
        return non_aggregated

    def _determine_size_opts(self, var_info: dict, agg_dims: list) -> list:
        """
        Determine the combinations of dimensions from the information
        around each variable. Each combination requires a different 
        ``location`` and ``shape`` fragment array variable in the final
        CFA-netCDF file.
        """

        cdimopts = []
        for v in var_info.values():
            cds = v['dims']

            if (set(agg_dims) & set(cds)):
                if cds and sorted(cds) not in cdimopts:
                    cdimopts.append(cds)

        logger.debug(f'Determined {len(cdimopts)} size options:')
        for c in cdimopts:
            logger.debug(f' - {tuple(c)}')
        return cdimopts

    def _accumulate_attrs(self, attrs: dict, ncattrs: dict) -> dict:
        """
        Accumulate attributes from the new source and the existing set.
        Ignore fill value attributes as these are handled elsewhere. 
        If attributes are not equal across files, the ``concat_msg`` is
        used to indicate where data users should seek out the source files
        for correct values.
        """

        if attrs is None:
            first_time = True
            attrs = {}

        for attr in ncattrs.keys():
            if attr == '_FillValue':
                continue

            if attr not in attrs:
                if first_time:
                    attrs[attr] = ncattrs[attr]
                else:
                    logger.warning(f'AttributeWarning: Attribute "{attr}" not present in all files')
                    attrs[attr] = self.concat_msg
            else:
                if attrs[attr] != ncattrs[attr]:
                    attrs[attr] = self.concat_msg
                else:
                    attrs[attr] = ncattrs[attr]
        return attrs

class CFAWriteMixin:
    
    """
    Mixin class for ``Write`` methods for a CFA-netCDF dataset.
    """

    def _write_dimensions(self):
        """
        Write the collected dimensions in dim_info as new dimensions
        in the CFA-netCDF file. So-called ``pure`` dimensions which 
        have no variable component (no array of values) are defined 
        with size alone, whereas coordinate dimensions (coordinate
        variables) have an associated variable component. The so-called
        ``f-dims`` are also created here as the fragmented size of each 
        coordinate dimension.

        Note: if a coordinate dimension is not fragmented, it still has
        an attributed f-dim, equal to 1.
        """

        f_dims = {}

        for dim, di in self.dim_info.items():

            f_size   = di['f_size']
            dim_size = di['size']

            real_part = self.ds.createDimension(
                dim,
                dim_size
            )

            frag_part = self.ds.createDimension(
                f'f_{dim}',
                f_size,
            )

            f_dims[f'f_{dim}'] = f_size

            if di['type'] == 'coord':
                axis_var = self.ds.createVariable(
                    dim,
                    di['dtype'],
                    (dim,), # Link to coord dimension
                )
                for k, v in di['attrs'].items():
                    axis_var.setncattr(k, v)

                axis_var[:] = di['array']
        
            else:
                for k, v in di['attrs'].items():
                    real_part.setncattr(k, v)

        return f_dims

    def _write_variables(self):
        """
        Non-aggregated variables are defined exactly the same as in
        the fragment files, while aggregated variables contain 
        ``aggregated_data`` and ``aggregated_dimensions`` attributes, 
        which link to the fragment array variables.
        """

        for var, meta in self.var_info.items():

            if 'adims' not in meta:
                variable = self._write_nonagg_variable(var, meta)
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
                    f'location: fragment_location_{num}',
                    f'address: fragment_address_{var}',
                    f'shape: fragment_shape_{num}'
                ])

                variable = self._write_aggregated_variable(var, meta, agg_dims, agg_data)

    def _write_fragment_addresses(self):
        """
        Create a ``fragment_address`` variable for each variable
        which is not dimension-less.
        """

        addrs = []
        for variable, meta in self.var_info.items():
            if 'adims' not in meta:
                continue
            addr = self.ds.createVariable(
                f'fragment_address_{variable}',
                str,
                (),
            )
            addr[:] = np.array(meta['address'], dtype=str)
            addrs.append(addr)

    def _write_shape_dims(self, f_dims: dict):
        """
        Construct the shape and location dimensions for each 
        combination of dimensions stored in ``cdim_opts``. This 
        utilises the  so-called ``f-dims`` previously created 
        for each coordinate dimension.
        """

        for x, opt in enumerate(self.cdim_opts):
            ndims = self.ds.createDimension(
                f'shape_{x}',
                len(opt),
            )

            vopt = tuple([f'f_{o}' for o in opt])

            if self.max_files > 1:
                vopt = vopt + ('versions',)

            location = self.ds.createVariable(
                f'fragment_location_{x}',
                str,
                vopt,
            )

            vshape = []
            for opt in vopt:
                vshape.append(f_dims[opt])

            loc_data = np.reshape(self.location, vshape)

            location[(slice(0, None) for i in vopt)] = np.array(loc_data, dtype=str)

    def _write_fragment_shapes(self):
        """
        Construct the ``fragment_shape`` variable part for each 
        combination of dimensions stored in ``cdim_opts``. This 
        utilises the ``shape`` dimensions previously created.
        """

        def fill_empty(array, size):
            array = list(array)
            init_length = int(len(array))
            for x in range(size - init_length):
                array.append(0)
            return tuple(array)

        cdimlens = {d: len(meta['sizes']) for d, meta in self.dim_info.items() if meta['type'] == 'coord'}

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
                if 'sizes' in self.dim_info[d]:
                    sizes = self.dim_info[d]['sizes']
                else:
                    sizes = (self.dim_info[d]['size'],)
                shapes.append(fill_empty(sizes, largest))

            shape = self.ds.createVariable(
                shape_name,
                int, # Type
                (f'shape_{num}', i_dim)
            )

            shapes = np.array(shapes)
            shapes = np.ma.array(shapes, dtype=int, mask=(shapes==0))
            shape[:,:] = shapes

    def _write_aggregated_variable(
            self, 
            var : str, 
            meta : dict, 
            agg_dims : str, 
            agg_data : str
        ):
        """
        Create the netCDF parameters required for an aggregated variable.

        Note: The dimensions and variables referenced in ``agg_data`` need to 
        have already been defined for the dataset by this point.
        """

        var_arr = self.ds.createVariable(
            var,
            meta['dtype'],
            (),
            fill_value = meta.pop('_FillValue', None),
        )

        for k, v in meta['attrs'].items():
            if k == '_FillValue':
                continue
            try:
                var_arr.setncattr(k, v)
            except Exception as err:
                logger.warning(
                    f'Cannot set attribute - {k}: {v} for {var}'
                )
                logger.warning(err)

        var_arr.aggregated_dimensions = agg_dims
        var_arr.aggregated_data = agg_data

    def _write_nonagg_variable(
            self, 
            var : str, 
            meta: dict
        ):

        """
        Create a non-aggregated variable for the CFA-netCDF file.
        If this variable has some attributed data (which it should),
        the data is set for this variable in the new file."""

        var_arr = self.ds.createVariable(
            var,
            meta['dtype'],
            meta['dims'],
            fill_value = meta.pop('_FillValue', None),
        )

        for k, v in meta['attrs'].items():
            if k == '_FillValue':
                continue
            try:
                var_arr.setncattr(k, v)
            except Exception as err:
                logger.warning(
                    f'Cannot set attribute - {k}: {v} for {var}'
                )
                logger.warning(err)
        
        if 'data' in meta:
            var_arr[:] = meta['data']

class CFANetCDF(CFACreateMixin, CFAWriteMixin):

    """
    CFA-netCDF file constructor class, enables creation and 
    writing of new CF1.12 aggregations.
    """

    description = 'The CFAPyX Constructor class, for creating new CFA-netCDF files.'

    def __init__(self, files: list, concat_msg : str = CONCAT_MSG):

        """
        Initialise this CFANetCDF instance with some basic values, and filter
        the provided set of files. A custom concat message can also be set
        here if needed."""

        if isinstance(files, str):
            fileset = glob.glob(files)
            self.files = self._filter_files(fileset)
            if len(self.files) < 2:
                raise ValueError(
                    f'Unable to aggregate less than two files; found {len(self.files)}'
                    f' from pattern "{files}"'
                )
        else:
            self.files = self._filter_files(files)
            if len(self.files) < 2:
                raise ValueError(
                    f'Unable to aggregate less than two files; only {len(self.files)}'
                    f' files given.'
                )

        self.longest_filename = ''

        self.global_attrs = None
        self.var_info     = None
        self.dim_info     = None

        self.fragment_space = None
        self.location = None
        self.cdim_opts = None

        self.concat_msg = concat_msg

        self.ds = None

    def create(
            self, 
            updates : dict = None,
            removals: list = None,
            agg_dims: list = None,
        ) -> None:

        """
        Perform the operations and passes needed to accumulate the set of
        variable/dimension info and attributes to then construct a CFA-netCDF
        file."""

        updates  = updates or {}
        removals = removals or []

        # First pass collect info
        arranged_files, global_attrs, var_info, dim_info = self._first_pass(agg_dims=agg_dims)

        global_attrs, var_info, dim_info = self._apply_filters(updates, removals, global_attrs, var_info, dim_info)
                    
        # Arrange aggregation dimensions
        dim_info, agg_dims = self._arrange_dimensions(dim_info, agg_dims=agg_dims)
        var_info = self._apply_agg_dims(var_info, agg_dims)

        # Determine size options and non-aggregated variables
        self.cdim_opts  = self._determine_size_opts(var_info, agg_dims)
        non_aggregated = self._determine_non_aggregated(var_info, agg_dims)

        # Perform a second pass to collect non-aggregated variables if present.
        if len(non_aggregated) > 0:
            var_info = self._second_pass(var_info, non_aggregated)
        
        # Define the fragment space
        self.fragment_space = [v['f_size'] for v in dim_info.values() if 'f_size' in v]

        # Assemble the location with correct dimensions
        location = self._assemble_location(arranged_files, dim_info)

        self.global_attrs = global_attrs
        self.dim_info   = dim_info
        self.var_info   = var_info
        self.location   = location

    def write(
            self, 
            outfile: str
        ) -> None:

        """
        Use the accumulated dimension/variable info and attributes to 
        construct a CFA-netCDF file."""

        self.ds = netCDF4.Dataset(outfile, mode='w', format='NETCDF4', maskandcale=True)
        self.ds.Conventions = 'CF-1.12'

        # Populate global dimensions
        for attr, value in self.global_attrs.items():
            self.ds.setncattr(attr, value)

        f_dims = self._write_dimensions()

        if self.max_files > 1:
            nfiles = self.ds.createDimension(
                'versions',
                self.max_files,
            )

            f_dims['versions'] = self.max_files

        self._write_shape_dims(f_dims)
        self._write_fragment_shapes()
        self._write_fragment_addresses()

        self._write_variables()

        self.ds.close()

    @property
    def agg_dims(self):
        """
        Display the aggregated dimensions identified on creation.
        """
        if not self.dim_info:
            return []
        
        agg_dims = []
        for dim, meta in self.dim_info.items():
            if 'f_size' not in meta:
                continue
            if meta['f_size'] > 1:
                agg_dims.append(dim)
        return tuple(agg_dims)
    
    @property
    def pure_dims(self):
        """
        Display the 'pure' dimensions identified on creation. Pure dimensions
        are defined only by a size, with no array of values.
        """
        if not self.dim_info:
            return []
        
        pure_dims = []
        for dim, meta in self.dim_info.items():
            if meta['type'] == 'pure':
                pure_dims.append(dim)
        return tuple(pure_dims)
    
    @property
    def coord_dims(self):
        """
        Display the coordinate dimensions identified on creation. Coordinate
        dimensions include an array of values for the dimension as a variable with
        the same name.
        """
        if not self.dim_info:
            return []
        
        coord_dims = []
        for dim, meta in self.dim_info.items():
            if meta['type'] == 'coord':
                coord_dims.append(dim)
        return tuple(coord_dims)

    @property
    def scalar_vars(self):
        """
        Display the scalar variables identified on creation, which are
        single valued and are dimensionless.
        """
        if not self.var_info:
            return []
        
        scalar_vars = []
        for var, meta in self.var_info.items():
            if 'dims' not in meta:
                scalar_vars.append(var)
            elif meta['dims'] == ():
                scalar_vars.append(var)

        return tuple(scalar_vars)

    @property
    def aggregated_vars(self):
        """
        Display the variables that vary across the aggregation dimensions.
        """
        if not self.var_info:
            return []
        
        agg_vars = []
        for var, meta in self.var_info.items():
            if 'adims' in meta:
                agg_vars.append(var)
        return tuple(agg_vars)

    @property
    def identical_vars(self):
        """
        Display the variables that do not vary across the aggregation 
        dimensions and must therefore be identical across all files.
        """
        if not self.var_info:
            return []
        
        id_vars = []
        for var, meta in self.var_info.items():
            if 'adims' not in meta:
                id_vars.append(var)
            elif meta['adims'] == ():
                id_vars.append(var)
        return tuple(id_vars)

    def _apply_filters(self, updates, removals, global_attrs, var_info, dim_info):

        global_attrs, var_info, dim_info = self._apply_updates(updates, global_attrs, var_info, dim_info)
        global_attrs, var_info, dim_info = self._apply_removals(removals, global_attrs, var_info, dim_info)

        return global_attrs, var_info, dim_info

    def _apply_updates(self, updates, global_attrs, var_info, dim_info):
        global_u, vars_u, dims_u = {}, {}, {}
        for upd in updates.keys():
            if '.' not in upd:
                global_u[upd] = updates[upd]
            else:
                item = upd.split('.')[0]
                if item in var_info.keys():
                    vars_u[upd] = updates[upd]
                elif item in dim_info.keys():
                    dims_u[upd] = updates[upd]
                else:
                    logger.warning(
                        'Attempting to set an attribute for a var/dim that'
                        f'is not present: "{item}"'
                    )
        
        for attr, upd in global_u.items():
            global_attrs[attr] = upd

        for attr, upd in vars_u.items():
            (v, vattr) = attr.split('.')
            var_info[v]['attrs'][vattr] = upd

        for attr, upd in dims_u.items():
            (d, dattr) = attr.split('.')
            dim_info[d]['attrs'][dattr] = upd

        return global_attrs, var_info, dim_info

    def _apply_removals(self, removals, global_attrs, var_info, dim_info):
        global_r, vars_r, dims_r = [],[],[]
        for rem in removals:
            if '.' not in rem:
                global_r.append(rem)
            else:
                item = rem.split('.')[0]
                if item in var_info.keys():
                    vars_r.append(rem)
                elif item in dim_info.keys():
                    dims_r.append(rem)
                else:
                    logger.warning(
                        'Attempting to remove an attribute for a var/dim that'
                        f'is not present: "{item}"'
                    )
        
        for rem in global_r:
            global_attrs.pop(rem)

        for rem in vars_r:
            (v, vattr) = rem.split('.')
            var_info[v]['attrs'].pop(rem)

        for rem in dims_r:
            (d, dattr) = rem.split('.')
            dim_info[v]['attrs'].pop(rem)

        return global_attrs, var_info, dim_info

    def _filter_files(self, files: list) -> list:
        """
        Filter the set of files to identify the trailing dimension
        indicative of multiple file locations. Also identifies the 
        length of the longest filename to be used later when storing
        numpy string arrays.
        """

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

    def _call_file(self, file: str) -> netCDF4.Dataset:
        """
        Open the file as a netcdf dataset. If there are multiple filenames
        provided, use the first file. Also determine the longest filename
        to be used to define the ``location`` parameter later.
        """

        if isinstance(file, tuple):
            ds = netCDF4.Dataset(file[0])
            for f in file:
                if len(f) > len(self.longest_filename):
                    self.longest_filename = f
        else:
            ds = netCDF4.Dataset(file)
            if len(file) > len(self.longest_filename):
                self.longest_filename = file

        return ds
