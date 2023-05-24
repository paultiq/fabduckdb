import duckdb
import fabduckdb  # type: ignore    # noqa
import pandas as pd
import numpy as np


# First set of tests are just basic: Don't Break Stuff
def test_param1():
    con = duckdb.connect()
    df = con.execute("select ?, ? from range(10)", [1, 2]).df()
    assert len(df) == 10 and len(df.columns) == 2


def test_namedparam1():
    con = duckdb.connect()
    df = con.execute("select $test from range(10)", {"test": 123}).df()
    assert len(df) == 10 and df.iloc[0][0] == 123


def test_positionalparam():
    con = duckdb.connect()
    df = con.execute("select $1, $1", [[333]], multiple_parameter_sets=True).df()
    assert len(df) == 1 and df.iloc[0][0] == 333


def test_param2statements():
    con = duckdb.connect()
    df = con.execute("select 123;select ?, ? from range(10)", [1, 2]).df()
    assert len(df) == 10 and len(df.columns) == 2


def test_param_multiple_paramgroups():
    con = duckdb.connect()
    df = con.execute(
        "select 123;select ?, ? from range(10)",
        [[], [1, 2]],
        multiple_parameter_sets=True,
    ).df()
    assert len(df) == 10 and len(df.columns) == 2


def test_named_param_multiple():
    con = duckdb.connect()

    df = con.execute(
        "select $boo;select $test from range(10)",
        [{"boo": 321}, {"test": 123}],
        multiple_parameter_sets=True,
    ).df()
    assert len(df) == 10 and df.iloc[0][0] == 123


# Testing Functions with Parameters


def test_named_function_param1():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, con: pd.DataFrame(np.random.rand(rows, 5)),
        generates_filepath=False,
    )
    df = duckdb.connect().execute("select * from dfcreate($test)", {"test": 3}).df()
    assert len(df) == 3 and len(df.columns) == 5


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
