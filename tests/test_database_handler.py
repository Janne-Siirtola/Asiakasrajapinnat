import pandas as pd
from asiakasrajapinnat_master.database_handler import DatabaseHandler, _InMemoryDriver


def test_upsert_and_fetch(tmp_path):
    base = {
        "A": {"name": "A", "dtype": "int"},
        "B": {"name": "B", "dtype": "float"},
    }
    driver = _InMemoryDriver()
    DatabaseHandler._instance = None
    db = DatabaseHandler(base_columns=base, driver=driver)
    customer = "testcust"

    df1 = pd.DataFrame({
        "TapahtumaId": ["1", "2"],
        "A": [10, 20],
        "B": [1.0, 2.0],
    })
    db.upsert_rows(customer, df1)

    df2 = pd.DataFrame({
        "TapahtumaId": ["2"],
        "A": [30],
        "B": [3.0],
    })
    db.upsert_rows(customer, df2)

    df = db.fetch_dataframe(customer).sort_values("TapahtumaId").reset_index(drop=True)
    assert len(df) == 2
    assert df.loc[1, "A"] == 30

    # add new column and ensure table is altered
    new_base = base | {"C": {"name": "C", "dtype": "string"}}
    db.ensure_table(customer, new_base)
    df3 = pd.DataFrame({
        "TapahtumaId": ["3"],
        "A": [5],
        "B": [1.5],
        "C": ["x"],
    })
    db.upsert_rows(customer, df3)
    df = db.fetch_dataframe(customer)
    assert "C" in df.columns
    assert df.loc[df["TapahtumaId"] == "3", "C"].iloc[0] == "x"


def test_tapahtumaid_not_duplicated(tmp_path):
    base = {
        "TAPWeightGuid": {"name": "TapahtumaId", "dtype": "string"},
        "A": {"name": "A", "dtype": "int"},
    }
    driver = _InMemoryDriver()
    DatabaseHandler._instance = None  # reset singleton
    db = DatabaseHandler(base_columns=base, driver=driver)
    customer = "testcust"

    df = pd.DataFrame({"TapahtumaId": ["1"], "A": [1]})
    db.upsert_rows(customer, df)
    result = db.fetch_dataframe(customer)
    assert list(result.columns) == ["TapahtumaId", "A"]
