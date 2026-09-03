class ArrayFileObjectHandler:
    """
    Wrapper class for netcdf-like methods when dealing with any file type.
    """

    def __init__(self, fh: object, mode: str):

        self.fh = fh
        self.mode = mode

    @property
    def units(self) -> str:
        """Return the units via standard class method"""

        match self.mode:
            case "pyfive":
                return self.fh.attrs.get("units")
            case "netcdf4":
                if hasattr(self.fh, "units"):
                    return self.fh.units
                return None
            case _:
                raise ValueError(f"Mode {self.mode} unknown")

    @property
    def shape(self) -> tuple:
        """Return the shape via standard class method"""
        return self.fh.shape

    @property
    def dimensions(self) -> tuple:
        """Return the dimensions via standard class method"""

        if hasattr(self.fh, "dimensions"):
            return tuple(self.fh.dimensions)

        match self.mode:
            case "pyfive":
                return tuple([dim[0].name.split("/")[-1] for dim in self.fh.dims])
            case _:
                raise ValueError(f"Mode {self.mode} unknown")

    def get_array(self) -> object:
        """
        Return array object from this wrapper."""
        return self.fh

    def apply_group(self, address: str) -> str:
        """Apply group internally."""

        if "/" not in address:
            return address

        match self.mode:
            case "pyfive":
                addr = address.split("/")
                for g in addr[1:-1]:
                    self.fh = self.fh[g]
                varname = addr[-1]

            case "netcdf4":
                addr = address.split("/")
                group = "/".join(addr[1:-1])
                varname = addr[-1]

                self.fh = self.fh.groups[group]
            case _:
                raise ValueError("Mode unknown")
        return varname

    def apply_variable(self, var) -> None:
        """Apply variable to internal array handler."""
        self.fh = self.fh[var]
