import duckdb
import fabduckdb  # type: ignore  # noqa


def test_range():
    query = "execute (select 'select * from range(' || x::varchar || ')' from range(3) t(x))"

    df = duckdb.default_connection.execute(query).df()

    assert len(df) == 3


def test_unionallbyname():
    statement = "execute (select 'select ' || i || ' as r' || i ||', x from range(' || i || ') t(x)' from range(4) t(i)) using ('union all by name')"
    df = duckdb.default_connection.execute(statement).df()

    assert len(df) == 6
    assert len(df.columns) == 5
