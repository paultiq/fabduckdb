import duckdb
import fabduckdb
import pandas as pd
import numpy as np


def test_simple():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, cols: pd.DataFrame(np.random.rand(rows, cols)),
        generates_filepath=False,
    )
    df = duckdb.connect().execute("select * from dfcreate(3,4)").df()
    assert len(df) == 3 and len(df.columns) == 4


def test_looped_function():
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, cols: pd.DataFrame(np.random.rand(rows, cols)),
        generates_filepath=False,
    )

    with duckdb.connect() as con:
        con.execute("create table abc as select x from range(1,5) t(x)")
        con.execute(
            "create table xyz as (loop (select * from dfcreate({{x}},{{x}})) over (select x from abc) using 'union all by name')"
        )
        df = con.execute("select * from xyz").df()
        assert len(df) == 10 and len(df.columns) == 4
