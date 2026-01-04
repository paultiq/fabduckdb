# from fabduckdb.fab_execute import registerfab
# from fabduckdb.decorator import wrap_execute
# from .fab_functions import register_function

from .fab_execute import registerfab
from .table_functions.fab_functions import register_function

registerfab()
