import pandas as pd
from asiakasrajapinnat_master.database_handler import DatabaseHandler


class _FakeDriver:
    """Simple in-memory driver for DatabaseHandler tests."""

    def __init__(self) -> None:
        self.tables: dict[str, pd.DataFrame] = {}

    def ensure_table(self, table_name: str, columns: dict[str, dict[str, str]]) -> None:
        if table_name not in self.tables:
            cols = ["TapahtumaId"] + [cfg["name"] for cfg in columns.values()]
            self.tables[table_name] = pd.DataFrame(columns=cols)
        else:
            df = self.tables[table_name]
            for cfg in columns.values():
                name = cfg["name"]
                if name not in df.columns:
                    df[name] = pd.Series(dtype="object")
            self.tables[table_name] = df

    def upsert_with_staging(self, table_name: str, df: pd.DataFrame, pk_col: str = "TapahtumaId") -> None:
        existing = self.tables.get(table_name)
        if existing is None:
            self.ensure_table(table_name, {c: {"name": c} for c in df.columns if c != pk_col})
            existing = self.tables[table_name]
        allowed = existing.columns
        df = df.loc[:, [c for c in df.columns if c in allowed]]
        merged = pd.concat([existing, df])
        merged = merged.drop_duplicates(subset=pk_col, keep="last")
        self.tables[table_name] = merged.reset_index(drop=True)

    def fetch_dataframe(self, table_name: str) -> pd.DataFrame:
        return self.tables.get(table_name, pd.DataFrame()).copy()


def test_upsert_and_fetch(tmp_path):
    base = {
        "A": {"name": "A", "dtype": "int"},
        "B": {"name": "B", "dtype": "float"},
    }
    driver = _FakeDriver()
    DatabaseHandler._instance = None
    db = DatabaseHandler(base_columns=base, driver=driver)
    customer = "testcust"

    df1 = pd.DataFrame({
        "TapahtumaId": ["1", "2"],
        "A": [10, 20],
        "B": [1.0, 2.0],
        "Paino": [1, 1],
    })
    db.upsert_rows(customer, df1)

    df2 = pd.DataFrame({
        "TapahtumaId": ["2"],
        "A": [30],
        "B": [3.0],
        "Paino": [1],
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
        "Paino": [1],
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
    driver = _FakeDriver()
    DatabaseHandler._instance = None  # reset singleton
    db = DatabaseHandler(base_columns=base, driver=driver)
    customer = "testcust"

    df = pd.DataFrame({"TapahtumaId": ["1"], "A": [1]})
    df["Paino"] = [1]
    db.upsert_rows(customer, df)
    result = db.fetch_dataframe(customer)
    assert list(result.columns) == ["TapahtumaId", "A"]
