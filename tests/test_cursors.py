import duckdb
import fabduckdb  # type: ignore    # noqa
import pandas as pd
import numpy as np


def test_concurrent_cursors():
    con = duckdb.connect()
    fabduckdb.register_function(
        "dfcreate",
        lambda rows, con: pd.DataFrame(np.random.rand(rows, 5)),
        generates_filepath=False,
    )

    cur = con.cursor()
    curp = cur.execute("select * from dfcreate(3)")
    cur2 = con.cursor()
    curp2 = cur2.execute("select * from dfcreate(3)")
    curp2 = cur2.execute("select * from dfcreate(1)")

    con.execute("select * from dfcreate(4)")

    assert len(curp.df()) == 3
    assert len(con.df()) == 4
    assert len(curp2.df()) == 1
