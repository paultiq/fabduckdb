import sqlparse
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
import re
import logging
import ast

logger = logging.getLogger(__name__)

_DFPREFIX = "__fabduck"


@dataclass
class RegisteredFunction:
    name: str
    func: object
    generates_filepath: bool
    file_extension: str = ".parquet"


@dataclass
class ContextObject:
    functionname: str
    functioncall: str
    params: str
    data: object
    name: Optional[str] = None
    is_file: bool = False


registered_functions = {}


_KPAT = ""


def register_function(name: str, func, generates_filepath: bool = False):
    """Filetype: If True, func returns a path to a parquet or CSV file. If False, func returns a dataframe or other registerable data type"""
    registered_functions[name] = RegisteredFunction(
        name=name, func=func, generates_filepath=generates_filepath
    )

    _keyword_list = "|".join(
        (k if isinstance(k, str) else k[0] for k in registered_functions.keys())
    )
    pattern = rf"(?s)({_keyword_list})(\.(\w+))?\s*\("

    global _KPAT
    _KPAT = re.compile(pattern)


def _find_closing_paren(text: str, start: int) -> int:
    # TODO: Handle escaping
    paren_depth = 1
    quote_stack = []
    for i in range(start, len(text)):
        c = text[i]
        if c == "(" and len(quote_stack) == 0:
            paren_depth += 1
        elif c == "'" or c == '"':
            if len(quote_stack) == 0:
                quote_stack.append(c)
            elif quote_stack[len(quote_stack) - 1] == c:
                quote_stack.remove(c)
        elif c == ")" and len(quote_stack) == 0:
            paren_depth -= 1

        if paren_depth == 0:
            end = i
            return end
    raise ValueError("Never found closing parenthesis, probably unbalanced query")


def _extract_subquery_strings(
    query, keywords: Optional[List[str]] = None
) -> List[ContextObject]:
    """Finds the subqueries start with keyword, along with matching end paren
    keyword(....)
    keyword.subword(....)
    """

    results = []
    for m in re.finditer(_KPAT, query):
        keyword = m.group(1)
        subword = m.group(3)
        paren_start = m.end()  # just after the last paren

        paren_end = _find_closing_paren(query, paren_start)

        outer = query[m.start() : paren_end + 1]
        inner = query[paren_start:paren_end]

        co = ContextObject(
            functionname=keyword, functioncall=outer, params=inner, data=None
        )

        logger.debug(
            f"Extracted subquery: {keyword}.{subword} at {paren_start}:{paren_end}"
        )

        # results.append((keyword, subword, outer, inner))
        results.append(co)

    return results


def extract_and_replace_functions(query) -> Tuple[str, Dict[str, ContextObject]]:
    if len(registered_functions) == 0:
        return (None, None)  # type: ignore

    subqueries: Dict[str, ContextObject] = {}

    i = 0

    replacements: Dict[str, Optional[str]] = {}

    if "--" in query:
        query = sqlparse.format(query, strip_comments=True).strip()
    else:
        query = query

    for co in _extract_subquery_strings(query):
        i += 1
        name = f"{_DFPREFIX}_{i}"
        # logger.info(keyword)

        co.name = name

        regfunc = registered_functions[co.functionname]
        if not regfunc.generates_filepath:
            replacements[co.functioncall] = name
            subqueries[name] = co
        else:  # csv or parquet
            name = name + regfunc.file_extension
            replacements[co.functioncall] = name
            subqueries[name] = co
            if len(co.params.strip()) >= 0:
                co.params += ","

            co.is_file = True
            co.params += f"filename='{name}'"

    for old, new in replacements.items():
        logger.info(f"Replacing {old} with {new}")
        query = query.replace(old, new)  # type: ignore

    # if only one function without a beginning subquery
    if len(replacements) > 0:
        values = [value for value in replacements.values() if value is not None]
        if re.match(rf"(?s)\s*({'|'.join(values)}).*", query) is not None:
            logger.info(f"Starts with replacement {values}")
            query = f"select * from {query}"

    return query, subqueries


def execute_allowlisted_function(
    functionname: str, params: str, con: object, statement_params
) -> object:
    logger.info(f"Running {functionname} against {params}")
    reg_function = registered_functions[functionname]
    function = reg_function.func
    # Define the parameter string
    param_string = f"f({params})"

    if statement_params is not None:
        subbed_param_string = param_string
        for k, v in statement_params.items():
            subbed_param_string = subbed_param_string.replace(f"${k}", str(v))

        # Replace statement params with any parameters
        logger.info(f"Params: {param_string} {subbed_param_string}")
        param_string = subbed_param_string

    # Parse the parameter string into an AST

    param_ast = ast.parse(param_string, mode="eval")

    # param_node = param_ast.body
    # Check if the expression node is a valid function call
    if isinstance(param_ast.body, ast.Constant):
        # Case: Single constant parameter
        param_value = ast.literal_eval(param_ast.body)
        con_args = {"con": con}
        result = function(param_value, **con_args)
        return result
    elif isinstance(param_ast.body, ast.Tuple):
        # Case: Constant positional arguments
        args = [ast.literal_eval(arg) for arg in param_ast.body.elts]
        con_args = {"con": con}
        result = function(*args, **con_args)
        return result
    elif isinstance(param_ast.body, ast.Dict):
        # Case: Keyword arguments (kwargs)
        kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in param_ast.body.keys}  # type: ignore
        kwargs["con"] = con
        result = function(**kwargs)
        return result
    elif isinstance(param_ast.body, ast.Call):
        # Case: Mix of keyword and positional arguments
        kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in param_ast.body.keywords}
        args = [ast.literal_eval(arg) for arg in param_ast.body.args]
        kwargs["con"] = con
        result = function(*args, **kwargs)
        return result


def _consume_functions(
    query: str, con, params
) -> Tuple[List[str], Dict[str, ContextObject]]:
    newquery, subqueries = extract_and_replace_functions(query)

    if subqueries is None or len(subqueries) == 0:
        return (None, None)  # type: ignore

    logger.info(f"{query} rewritten to {newquery}, {subqueries}")

    for k, co in subqueries.items():
        logger.info(f"Executing subquery {co.functioncall}")
        result = execute_allowlisted_function(
            co.functionname, co.params, con=con, statement_params=params
        )
        co.data = result

    return ([newquery], subqueries)
