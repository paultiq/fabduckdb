# missing clauses
# mismatch in on and over
import duckdb
import fabduckdb  # type: ignore    # noqa


def test_loop1():
    createstatement = "CREATE or REPLACE TABLE abc as SELECT * FROM range(1,10,1) t(x);CREATE or REPLACE TABLE def as SELECT * FROM range(20,30,2) t(x)"
    loopstatement = (
        "loop (select * from {{x}}) over (select x from (values('abc'), ('def')) t(x))"
    )
    duckdb.default_connection.execute(createstatement)
    df = duckdb.default_connection.execute(loopstatement).df()
    assert len(df) == 14


def test_embedded_loop():
    createstatement = "CREATE or REPLACE TABLE abc as SELECT * FROM range(1,10,1) t(x);CREATE or REPLACE TABLE def as SELECT * FROM range(20,30,2) t(x)"
    loopstatement = "create table xyz as (loop (select * from {{x}}) over (select x from (values('abc'), ('def')) t(x)))"
    duckdb.default_connection.execute(createstatement)
    duckdb.default_connection.execute(loopstatement)
    df = duckdb.default_connection.execute("select * from xyz").df()
    assert len(df) == 14
