import logging
from collections import deque
from typing import Dict, List, Optional, Tuple

from duckdb import DuckDBPyConnection

from fabduckdb.fab_functions import ContextObject, _consume_functions
from fabduckdb.fab_statements import _consume_statements
from fabduckdb.splitter import split_into_statements

PATCHED = False
DONTPATCH = False

logger = logging.getLogger(__name__)


def register():
    global PATCHED
    if PATCHED or DONTPATCH:
        return
    else:
        if not hasattr(DuckDBPyConnection, "execute_decorated"):
            logger.info("Patching")

            DuckDBPyConnection._execute_orig_ = DuckDBPyConnection.execute  # type: ignore
            DuckDBPyConnection.execute_decorated = DuckDBPyConnection.execute  # type: ignore

        DuckDBPyConnection.execute = fab_execute
        PATCHED = True


lasttokens = None


def _consume(query: str, con) -> Tuple[Optional[List[str]], Dict[str, ContextObject]]:
    statements, context = _consume_statements(query, con)

    if statements is not None:
        return statements, context
    else:
        # After doing any statement rewriting, see if there's any functions to replace
        statements, context = _consume_functions(query, con)

        return statements, context


items_to_unregister = []
ready_to_unregister = False


def fab_execute(self, query: str, parameters: object = None, multiple_parameter_sets: bool = False) -> DuckDBPyConnection:  # type: ignore
    """I am your captain now."""
    register()
    global items_to_unregister
    global ready_to_unregister

    if ready_to_unregister and len(items_to_unregister) > 0:
        for i in items_to_unregister:
            self.unregister(i)
        items_to_unregister = []

    # TODO: Strip Comments

    # TODO: Don't split on semicolon
    statements = split_into_statements(query)
    logger.info(f"# Statements: {len(statements)}, {statements}")
    statementQueue = deque(
        statements
    )  # deque because inserting is from the left, not right.

    items_to_unregister = []
    res = None
    while statementQueue:
        statement = statementQueue.popleft()
        logger.info(f"Processing: {statement}")

        (new_statements, new_context) = _consume(statement, con=self)

        # If there are new context items, register them
        if new_context is not None:
            for k, v in new_context.items():
                if not v.is_file:
                    if v.data is None:
                        raise ValueError(f"{k} is None, cannot process")
                    logger.info(f"Registering {k}")
                    self.register(k, v.data)
                    items_to_unregister.append(k)
                    # TODO: Delete any files

        if new_statements is None:  # run as is
            res = self.execute_decorated(query=statement, parameters=parameters, multiple_parameter_sets=multiple_parameter_sets)  # type: ignore

            # can't unregister them until the next statement: the data must be consumed first
            ready_to_unregister = True

        else:
            logger.info(f"Rewrote: {statement} => {new_statements}")

            list(map(statementQueue.appendleft, new_statements))

    return res  # type: ignore
