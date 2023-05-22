import sqlparse
from typing import List


def split_into_statements(query: str) -> List[str]:
    return [statement for statement in sqlparse.split(query)]  # type: ignore
