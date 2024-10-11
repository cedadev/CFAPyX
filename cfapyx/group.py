__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import logging

logger = logging.getLogger(__name__)

class VariableWrapper:
    """
    Wrapper object for the ``ds.variables`` and ``ds.attributes`` objects which can handle
    either ``global`` or ``group`` based variables .
    """

    def __init__(self, prop_sets):

        # Note: core_ds refers to either the group, or the global ds if there is no group requested.

        self._core_props = prop_sets[0]

        props = {}
        for prop in prop_sets:
            props = props | prop

        self._properties = props

    def __getitem__(self, item):
        """
        Requesting a named or indexed variable within ds.variables, needs
        to be handled for the two sets of variables.
        """

        if type(item) == int:
            item = list(self.keys())[item]

        if item in self._properties:
            return self._properties[item]
        else:
            raise ValueError(
                f'"{item}" not present in Dataset.'
            )
        
    def keys(self):
        """
        Requesting the set of keys should return the set of both keys combined in a ``dict_keys`` object.
        """
        return self._properties.keys()
    
    def items(self):
        return self._properties.items()

    def __getattr__(self, attr):
        try:
            return getattr(self._core_props, attr)
        except:
            raise AttributeError(
                f'No such attribute: "{attr}'
            )
        
class CFAGroupWrapper:
    """
    Wrapper object for the CFADataStore ``ds`` parameter, required to bypass the issue
    with groups in Xarray, where all variables outside the group are ignored.
    """
    def __init__(self, var_sets, ds_sets):

        self.variables = VariableWrapper(
            var_sets,
        )

        self._ds_sets = ds_sets

        self.Conventions = ''
        if hasattr(ds_sets[0],'Conventions'):
            self.Conventions = ds_sets[0].Conventions

    @classmethod
    def open(cls, root, group, mode):

        if group in {None, "", "/"}:
            # use the root group
            return root
        else:
            # make sure it's a string
            if not isinstance(group, str):
                raise ValueError("group must be a string or None")
            
            # support path-like syntax
            path = group.strip("/").split("/")
            var_sets = [root.variables]
            ds_sets = [root]

            for part in path:
                try:
                    var_sets.append(root.groups[part].variables)
                    ds_sets.append(root.groups[part])
                    root = root.groups[part]
                except KeyError as e:
                    raise ValueError(
                        f'Group path "{part}" not found in this dataset.'
                    )

        return cls(var_sets, ds_sets)

    @property
    def dimensions(self):
        dims = {}
        for ds in self._ds_sets:
            dims = dims | dict(ds.dimensions)
        return dims

    def ncattrs(self):
        attrs = []
        for ds in self._ds_sets:
            attrs += list(ds.ncattrs()) # Determine return type
        return attrs

    def getncattr(self, k):
        for ds in self._ds_sets:
            try:
                return ds.getncattr(k)
            except:
                pass
        raise AttributeError(
            f'Attribute "{k}" not found.'
        )

    @property
    def variables(self):
        return self._variables
    
    @variables.setter
    def variables(self, value):
        self._variables = value

    def __getattr__(self, name):
        for ds in self._ds_sets:
            try:
                return getattr(ds, name)
            except:
                pass
        raise AttributeError(
            f'Attribute "{name}" not found.'
        )