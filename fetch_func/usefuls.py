## NOTE: Not a script which is meant to be run


def ensure_not_multiindex(var: Variable, name: T_Name = None) -> None:
    # only the pandas multi-index dimension coordinate cannot be serialized (tuple values)
    if isinstance(var._data, indexing.PandasMultiIndexingAdapter):
        if name is None and isinstance(var, IndexVariable):
            name = var.name
        if var.dims == (name,):
            raise NotImplementedError(
                f"variable {name!r} is a MultiIndex, which cannot yet be "
                "serialized. Instead, either use reset_index() "
                "to convert MultiIndex levels into coordinate variables instead "
                "or use https://cf-xarray.readthedocs.io/en/latest/coding.html."
            )


