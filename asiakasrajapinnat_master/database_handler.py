"""Azure SQL database handler for storing customer data."""

from __future__ import annotations

import logging
import os
from typing import Dict, List

import numpy as np
import pyodbc

import pandas as pd
import importlib
from urllib import parse



class DatabaseHandler:
    """Singleton wrapper around an Azure SQL database."""

    _instance: "DatabaseHandler | None" = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(
        self,
        base_columns: Dict[str, Dict[str, str]] | None = None,
        driver: object | None = None,
        local_test: bool = False,
    ) -> None:
        if self._initialized:
            return
        if driver is not None:
            self.driver = driver
        else:
            sa = importlib.import_module("sqlalchemy")
            server = os.getenv("SQL_SERVER")
            database = os.getenv("SQL_DATABASE")
            odbc_driver = "{ODBC Driver 18 for SQL Server}"
            if local_test:
                usr = os.getenv("SQL_USERNAME")
                pwd = os.getenv("SQL_PASSWORD")
                conn_str = (
                    f"DRIVER={odbc_driver};SERVER={server};DATABASE={database};"
                    f"UID={usr};PWD={pwd};Encrypt=yes;TrustServerCertificate=no;"
                )
            else:
                conn_str = (
                    f"DRIVER={odbc_driver};SERVER={server};DATABASE={database};"
                    "Authentication=ActiveDirectoryMsi;Encrypt=yes;"
                )

            params = parse.quote_plus(conn_str)
            self.engine = sa.create_engine(
                url=f"mssql+pyodbc:///?odbc_connect={params}",
                fast_executemany=True,
            )
            self.schema = "esrs"
            self.sa = sa
            self.driver = self
        self.base_columns = self._filter_columns(base_columns or {})
        self._initialized = True

    # -- internal helpers -------------------------------------------------
    @staticmethod
    def _filter_columns(columns: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """Return ``columns`` without entries whose name is 'TapahtumaId'."""
        return {
            key: cfg
            for key, cfg in columns.items()
            if cfg.get("name") != "TapahtumaId"
        }

    @staticmethod
    def _sanitize(name: str) -> str:
        return name.replace("-", "_").replace(" ", "_")

    # -- internal Azure operations -------------------------------------
    def _get_columns_config(self, columns: Dict[str, Dict[str, str]]):
        cols: List[dict] = []
        cols.append({
            "name": "TapahtumaId",
            "type_": self.sa.String(255),
            "kwargs": {"primary_key": True},
        })

        for col in columns.values():
            name = col["name"]
            dtype_s = col["dtype"].lower()
            kwargs: Dict[str, object] = {}

            if dtype_s.startswith("int"):
                type_ = self.sa.BigInteger()
            elif dtype_s.startswith("float"):
                dec = col.get("decimals")
                if dec is not None:
                    type_ = self.sa.Numeric(precision=16, scale=int(dec))
                else:
                    type_ = self.sa.Float()
            elif dtype_s.startswith("string"):
                length = col.get("length")
                type_ = self.sa.String(int(length)) if length else self.sa.String(255)
            else:
                type_ = self.sa.String(255)

            if "nullable" in col:
                kwargs["nullable"] = bool(col["nullable"])
            if "default" in col:
                kwargs["default"] = col["default"]

            cols.append({"name": name, "type_": type_, "kwargs": kwargs})

        return cols

    def _ensure_table_sql(self, table_name: str, columns: Dict[str, Dict[str, str]]):
        inspector = self.sa.inspect(self.engine)
        exists = inspector.has_table(table_name, schema=self.schema)

        metadata = self.sa.MetaData(schema=self.schema)
        tbl = self.sa.Table(table_name, metadata)
        for cfg in self._get_columns_config(columns):
            tbl.append_column(self.sa.Column(cfg["name"], cfg["type_"], **cfg["kwargs"]))
        metadata.create_all(self.engine)

        if exists:
            existing = self.sa.Table(
                table_name,
                self.sa.MetaData(),
                schema=self.schema,
                autoload_with=self.engine,
            )
            existing_cols = set(existing.columns.keys())

            with self.engine.begin() as conn:
                for cfg in self._get_columns_config(columns):
                    name = cfg["name"]
                    if name in existing_cols:
                        continue
                    ddl_type = cfg["type_"].compile(dialect=self.engine.dialect)
                    stmt = self.sa.text(
                        f"ALTER TABLE [{self.schema}].[{table_name}] ADD [{name}] {ddl_type}"
                    )
                    conn.execute(stmt)


    def _upsert_with_staging(self, table_name: str, df: pd.DataFrame, pk_col: str = "TapahtumaId") -> None:
        staging = f"{table_name}_stg"
        schema = self.schema
        engine = self.engine

        df_clean = df.where(pd.notnull(df), None)
        df_clean.to_sql(
            name=staging,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method=None,
            chunksize=2000,
        )

        target = f"[{schema}].[{table_name}]"
        src = f"[{schema}].[{staging}]"
        meta = self.sa.MetaData(schema=schema)
        tbl = self.sa.Table(table_name, meta, autoload_with=engine)
        all_cols = [c.name for c in tbl.columns]
        non_pk_cols = [c for c in all_cols if c != pk_col]

        update_set = ",\n    ".join(f"t.[{c}] = s.[{c}]" for c in non_pk_cols)
        cols_list = ", ".join(f"[{c}]" for c in all_cols)
        src_cols = ", ".join(f"s.[{c}]" for c in all_cols)

        merge_sql = f"""
            MERGE INTO {target} AS t
            USING {src} AS s
                ON t.[{pk_col}] = s.[{pk_col}]
            WHEN MATCHED THEN
            UPDATE SET
                {update_set}
            WHEN NOT MATCHED THEN
            INSERT ({cols_list})
            VALUES ({src_cols});
        """

        with engine.begin() as conn:
            conn.execute(self.sa.text(merge_sql))
            conn.execute(self.sa.text(f"DROP TABLE {schema}.{staging}"))

    def _fetch_dataframe_sql(self, table_name: str) -> pd.DataFrame:
        df = pd.read_sql(
            sql=f"SELECT * FROM [{self.schema}].[{table_name}]",
            con=self.engine,
        )
        return df

    # -- public API -------------------------------------------------------
    def ensure_table(
        self,
        customer: str,
        base_columns: Dict[str, Dict[str, str]] | None = None,
    ) -> None:
        if base_columns is not None:
            self.base_columns.update(self._filter_columns(base_columns))
        columns = self.base_columns
        table = self._sanitize(customer)
        if self.driver is self:
            self._ensure_table_sql(table, columns)
        else:
            self.driver.ensure_table(table, columns)
        
    def upsert_rows(
        self,
        customer: str,
        df: pd.DataFrame,
    ) -> None:
        table = self._sanitize(customer)
        if "TapahtumaId" not in df.columns:
            raise ValueError("DataFrame must contain 'TapahtumaId' column.")
        self.ensure_table(customer=customer)
        df = df[df["Paino"] != 0].copy()
        if self.driver is self:
            self._upsert_with_staging(table_name=table, df=df)
        else:
            self.driver.upsert_with_staging(table_name=table, df=df)
        
    def fetch_dataframe(self, customer: str) -> pd.DataFrame:
        table = self._sanitize(customer)
        if self.driver is self:
            return self._fetch_dataframe_sql(table_name=table)
        return self.driver.fetch_dataframe(table_name=table)


