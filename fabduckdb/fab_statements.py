import logging
from typing import Dict, List, Optional, Tuple

import sqlparse
from jinja2 import Template
from sqlparse.tokens import Whitespace

from fabduckdb.fab_functions import ContextObject

logger = logging.getLogger(__name__)
DEFAULT_HOW = "UNION ALL"


def threaded_statement(query: str):
    return None


def execute_statement(query: str, tokens: list[sqlparse.tokens.Token], con) -> Tuple[Optional[List[str]], Dict[str, ContextObject]]:  # type: ignore
    # firsttoken=tokens[0].value.upper()

    if len(tokens) != 2 and len(tokens) != 4:
        raise ValueError(
            f"Unexpected Syntax: Expected two or four tokens: execute (statement) [USING 'method']. {len(tokens)} {[t.value for t in tokens]}"
        )

    arguments = {}
    for i in range(0, len(tokens) - 1, 2):
        arguments[tokens[i].value.upper()] = tokens[i + 1].value

    execute_statement = arguments["EXECUTE"]
    how = arguments.get("USING", DEFAULT_HOW).strip("()'\"")
    execute_results = con._execute_orig_(execute_statement).arrow()

    queries = execute_results.column(0).to_pylist()

    if how == "None":
        return (queries, {})
    else:
        return ([f" {how} ".join(queries)], {})


def find_loop_statements(parsedquery):
    s1 = parsedquery[0]
    loops = []

    def process_paren(token):
        # print(f"Processing paren {token.value}")
        inner_tokens = token.tokens

        inner_tokens = inner_tokens[1:-1]
        # print(inner_tokens)
        # If the first token is 'loop'
        if inner_tokens[0].value.upper() == "LOOP":
            loops.append(
                token.value[1:-1].strip()
            )  # return the string, but strip off the enclosing parens
            return

    # Traverse through the parsed tokens
    for token in s1.tokens:
        # print(token.value, token.ttype)
        # If the token is a Parenthesis

        if isinstance(token, sqlparse.sql.Parenthesis):
            process_paren(token)

        if hasattr(token, "get_sublists"):
            for sl in token.get_sublists():
                if isinstance(sl, sqlparse.sql.Parenthesis):
                    process_paren(sl)

    if len(loops) > 0:
        logger.info(f"Found loops: {loops}")
        return loops
    else:
        return None


def loop_statement(query: str, tokens: list[sqlparse.tokens.Token], con) -> Tuple[Optional[List[str]], Dict[str, ContextObject]]:  # type: ignore
    """Uses the OVER statement as parameters to the Jinja2 templatized statement"""

    firsttoken = tokens[0].value.upper()

    if firsttoken != "LOOP":
        raise ValueError(f"Unexpected, must be a LOOP statement: {firsttoken}")
    else:
        if len(tokens) % 2 != 0:
            raise ValueError(
                f"Unexpected Syntax: there should be an even number of tokens, but got {len(tokens)}: LOOP (statement) OVER (statement) [USING 'method']. Got {[t.value for t in tokens]}"
            )
        else:
            logger.info(f"Let's loopify {query}, with tokens {tokens}, {len(tokens)}")

            arguments = {}
            for i in range(0, len(tokens) - 1, 2):
                arguments[tokens[i].value.upper()] = tokens[i + 1].value

            # validate input
            if "LOOP" not in arguments or "OVER" not in arguments:
                raise ValueError("Loop statement must have an ON and OVER clause")

            loop = arguments["LOOP"]
            over = arguments["OVER"]
            how = arguments.get("USING", DEFAULT_HOW).strip("()'\"")

            # Execute the "over" clause
            over_results = con._execute_orig_(over).arrow()

            queries = [Template(loop).render(row) for row in over_results.to_pylist()]

            if how == "None":
                return (queries, {})
            else:
                return ([f" {how} ".join(queries)], {})


STATEMENT_MAP = {
    "LOOP": loop_statement,
    "EXECUTE": execute_statement,
    "THREADED": threaded_statement,
}


def get_tokenlist(parsedquery):
    tokens = [
        t
        for t in parsedquery[0].tokens
        if t.ttype != Whitespace and not t.is_whitespace
    ]
    return tokens


def _consume_statements(
    query: str, con
) -> Tuple[Optional[List[str]], Dict[str, ContextObject]]:
    firstword = query.split(maxsplit=1)[0].upper()
    parsedquery = sqlparse.parse(query)

    loops = find_loop_statements(parsedquery)
    if loops is not None:
        for loop in loops:
            replacement = STATEMENT_MAP["LOOP"](
                loop, get_tokenlist(sqlparse.parse(loop)), con
            )
            query = query.replace(loop, replacement[0][0])
        return ([query], {})

    if firstword in STATEMENT_MAP:
        global lasttokens
        tokens = get_tokenlist(parsedquery)
        lasttokens = tokens

        return STATEMENT_MAP[firstword](query, tokens, con)
    else:
        return (None, {})
