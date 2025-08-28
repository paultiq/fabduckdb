# missing clauses
# mismatch in on and over
import duckdb
import fabduckdb  # type: ignore    # noqa


def test_loop1():
    createstatement = "CREATE or REPLACE TABLE abc as SELECT * FROM range(1,10,1) t(x);CREATE or REPLACE TABLE def as SELECT * FROM range(20,30,2) t(x)"
    loopstatement = (
        "loop (select * from {{x}}) over (select x from (values('abc'), ('def')) t(x))"
    )
    with duckdb.connect() as con:
        con.execute(createstatement)
        df = con.execute(loopstatement).df()

    assert len(df) == 14


def test_embedded_loop():
    createstatement = "CREATE or REPLACE TABLE abc as SELECT * FROM range(1,10,1) t(x);CREATE or REPLACE TABLE def as SELECT * FROM range(20,30,2) t(x)"
    loopstatement = "create table xyz as (loop (select * from {{x}}) over (select x from (values('abc'), ('def')) t(x)))"
    duckdb.default_connection.execute(createstatement)
    duckdb.default_connection.execute(loopstatement)
    df = duckdb.default_connection.execute("select * from xyz").df()
    assert len(df) == 14


def test_looped_function():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, cols, con=None: pd.DataFrame(np.random.rand(rows, cols)),
        generates_filepath=False,
    )

    with duckdb.connect() as con:
        con.execute("create table abc as select x from range(1,5) t(x)")
        con.execute(
            "create table xyz as (loop (select * from dfcreate({{x}},{{x}})) over (select x from abc) using 'union all by name')"
        )
        df = con.execute("select * from xyz").df()
        assert len(df) == 10 and len(df.columns) == 4


def test_named_function_loop():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, con: pd.DataFrame(np.random.rand(rows, 5)),
        generates_filepath=False,
    )
    df = (
        duckdb.connect()
        .execute(
            "loop (select $test, {{x}}) over (select x from range(4) t(x)) using 'union all'",
            {"test": "my value"},
        )
        .df()
    )

    assert len(df) == 4 and len(df.columns) == 2 and df.iloc[0][0] == "my value"
