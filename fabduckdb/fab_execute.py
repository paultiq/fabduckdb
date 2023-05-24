import logging
from collections import deque
from typing import Dict, List, Optional, Tuple

from duckdb import DuckDBPyConnection
import duckdb
from fabduckdb.fab_functions import ContextObject, _consume_functions
from fabduckdb.fab_statements import _consume_statements
from fabduckdb.splitter import split_into_statements

PATCHED = False
DONTPATCH = False

logger = logging.getLogger(__name__)


def registerfab():
    """Patches DuckDBPyConnection with a custom executor. The original executor is saved, and called by the custom executor.
    Also replaces duckdb.execute() with duckdb.default_connection.execute(), which ensures duckbdb.execute() uses the custom executor path, same as other connections.
    """

    global PATCHED
    if PATCHED or DONTPATCH:
        return
    else:
        if not hasattr(DuckDBPyConnection, "execute_decorated"):
            logger.info("Patching")

            DuckDBPyConnection._execute_orig_ = DuckDBPyConnection.execute  # type: ignore
            DuckDBPyConnection.execute_decorated = DuckDBPyConnection.execute  # type: ignore

        DuckDBPyConnection._fabitems_to_unregister = {}  # type: ignore
        DuckDBPyConnection.execute = fab_execute

        # This replaces execute with a Python-native function.  execute, so it's patchable.
        duckdb.execute = lambda sql: duckdb.default_connection.execute(sql)

        PATCHED = True


lasttokens = None


def _consume(
    query: str, con, params
) -> Tuple[Optional[List[str]], Dict[str, ContextObject]]:
    statements, context = _consume_statements(query, con, params)

    if statements is not None:
        return statements, context
    else:
        # After doing any statement rewriting, see if there's any functions to replace
        statements, context = _consume_functions(query, con, params)

        return statements, context


def process_top_level_statement(con, statement: str, params: object) -> object:
    statementQueue = deque(
        [statement]
    )  # deque because inserting is from the left, not right.

    res = None

    while statementQueue:
        statement = statementQueue.popleft()
        logger.info(f"Processing: {statement}")

        (new_statements, new_context) = _consume(statement, con=con, params=params)

        # If there are new context items, register them
        if new_context is not None:
            for k, v in new_context.items():
                if not v.is_file:
                    if v.data is None:
                        raise ValueError(f"{k} is None, cannot process")
                    logger.info(f"Registering {k}")
                    con.register(k, v.data)
                    DuckDBPyConnection._fabitems_to_unregister[con].append(k)  # type: ignore
                    # TODO: Delete any files

        if new_statements is None:  # run as is
            if isinstance(params, dict):
                # Only pass named params that are used
                params = {k: v for k, v in params.items() if str(k) in statement}
                if len(params) == 0:
                    params = None
            res = con.execute_decorated(query=statement, parameters=params, multiple_parameter_sets=False)  # type: ignore
            # can't unregister them until the next statement: the data must be consumed first

        else:
            logger.info(f"Rewrote: {statement} => {new_statements}")

            list(map(statementQueue.appendleft, new_statements))

    return res  # type: ignore


def convert_questionmark_parameters(statement):
    # TODO: Convert ? parameters to $1, $2, etc, so we can use them in functions, loops, etc
    if "?" in statement:
        print("Use named or positional ($1) parameters, not anonymous parameters")
    return statement


def fab_execute(self, query: str, parameters: object = None, multiple_parameter_sets: bool = False) -> DuckDBPyConnection:  # type: ignore
    """I am your captain now."""

    registerfab()

    # TODO: Don't split on semicolon
    statements = split_into_statements(query)

    logger.info(f"# Statements: {len(statements)}, {statements}")
    res = self

    if self not in DuckDBPyConnection._fabitems_to_unregister:  # type: ignore
        DuckDBPyConnection._fabitems_to_unregister[self] = []  # type: ignore

    for query_index, statement in enumerate(statements):
        # clear anything from the last run

        if len(DuckDBPyConnection._fabitems_to_unregister[self]) > 0:  # type: ignore
            for i in DuckDBPyConnection._fabitems_to_unregister[self]:  # type: ignore
                self.unregister(i)
            DuckDBPyConnection._fabitems_to_unregister[self] = []  # type: ignore

        # Get params
        if multiple_parameter_sets:
            params = parameters[query_index]  # type: ignore
        else:
            if query_index == len(statements) - 1:
                params = parameters
            else:
                params = None

        # Process the statement
        statement = convert_questionmark_parameters(statement)

        # The top level statement model is used so we can use the same params for everything inside the statement
        res = process_top_level_statement(con=self, statement=statement, params=params)
    # cleanup

    return res  # type: ignore
