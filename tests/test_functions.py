import duckdb
import fabduckdb
import pandas as pd
import numpy as np


def test_onepos():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, con: pd.DataFrame(np.random.rand(rows, 5)),
        generates_filepath=False,
    )
    df = duckdb.connect().execute("select * from dfcreate(3)").df()
    assert len(df) == 3 and len(df.columns) == 5


def test_twopos():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, cols, **_: pd.DataFrame(np.random.rand(rows, cols)),
        generates_filepath=False,
    )
    df = duckdb.connect().execute("select * from dfcreate(3,4)").df()
    assert len(df) == 3 and len(df.columns) == 4


def test_oneposkw():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, con: pd.DataFrame(np.random.rand(rows, 5)),
        generates_filepath=False,
    )
    df = duckdb.connect().execute("select * from dfcreate(rows=3)").df()
    assert len(df) == 3 and len(df.columns) == 5


def test_oneposkw_defaults():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows=5, columns=2, con=None: pd.DataFrame(np.random.rand(rows, columns)),
        generates_filepath=False,
    )
    df = duckdb.connect().execute("select * from dfcreate(rows=4)").df()
    assert len(df) == 4 and len(df.columns) == 2


def test_oneposkw_defaults_string():
    fabduckdb.register_function(
        "dfcreate",
        lambda name, rows=5, columns=2, con=None: pd.DataFrame(
            np.random.rand(rows, columns)
        ).assign(name=name),
        generates_filepath=False,
    )
    df = duckdb.connect().execute("select * from dfcreate(name='asd', rows=4)").df()
    assert len(df) == 4 and len(df.columns) == 3


def test_oneposkw_longstring():
    fabduckdb.register_function(
        "dfcreate",
        lambda name, rows=5, columns=2, con=None: pd.DataFrame(
            np.random.rand(rows, columns)
        ).assign(name=name),
        generates_filepath=False,
    )
    df = (
        duckdb.connect()
        .execute(
            """select * from dfcreate(name='''asd
                                        asd
                                        asd
                                        asd''', rows=4)"""
        )
        .df()
    )
    assert len(df) == 4 and len(df.columns) == 3


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
