from duckdb import DuckDBPyConnection


def _wrap(funcname, newfunc, append: bool = False):
    # Stores the original function in _func_orig_
    # If append = False, then uses _func_orig_ on any subsequent calls
    if funcname == "execute":  # execute is a special case, since fab_execute wraps it
        funcname = "execute_decorated"
        orig_funcname = "_execute_orig_"
    else:
        orig_funcname = "_" + funcname + "_orig_"

    if not hasattr(DuckDBPyConnection, funcname):
        raise ValueError(f"DuckDBPyConnection does not have function {funcname}")

    if not hasattr(DuckDBPyConnection, orig_funcname):
        orig_func = getattr(DuckDBPyConnection, funcname)
        setattr(DuckDBPyConnection, orig_funcname, orig_func)
    else:
        orig_func = getattr(DuckDBPyConnection, orig_funcname)

    if append:  # wraps the original function
        base_func = getattr(DuckDBPyConnection, funcname)
    else:
        base_func = orig_func

    replacement_func = newfunc(base_func)

    setattr(DuckDBPyConnection, funcname, replacement_func)


def wrap_execute(func, append: bool = False):
    _wrap(funcname="execute", newfunc=func, append=append)
