"""Azure SQL database handler for storing customer data."""

from __future__ import annotations

import logging
import os
from typing import Dict, List

import numpy as np
import pyodbc

import pandas as pd
from sqlalchemy import (
    create_engine, Table, Column, MetaData, Integer, BigInteger, String, Float, Numeric, Date, inspect, text
)
from urllib import parse


class _AzureDriver:
    """Driver that performs operations against Azure SQL via pyodbc."""

    def __init__(self, server: str, database: str, driver: str, local_test: bool = False) -> None:
        if local_test:
            USR = os.getenv('SQL_USERNAME')
            PWD = os.getenv("SQL_PASSWORD")
            
            conn_str = (
                f"DRIVER={driver};SERVER={server};DATABASE={database};"
                f"UID={USR};PWD={PWD};Encrypt=yes;TrustServerCertificate=no;")
        else:
            conn_str = (
                f"DRIVER={driver};SERVER={server};DATABASE={database};"
                "Authentication=ActiveDirectoryMsi;Encrypt=yes;"
            )
        params = parse.quote_plus(conn_str)
        self.engine = create_engine(
            url=f"mssql+pyodbc:///?odbc_connect={params}",
            fast_executemany=True
            )
            
        self.schema = "esrs"

    
    def get_columns_config(self, columns: dict[str, dict[str, str]]):
        """
        Build a list of column configs where each config has:
          - 'name'   : column name
          - 'type_'  : a SQLAlchemy TypeEngine instance
          - 'kwargs' : dict of Column kwargs (nullable, primary_key, default…)
        """
        cols = []
        # always include your PK
        cols.append({
            "name": "TapahtumaId",
            "type_": String(255),
            "kwargs": {"primary_key": True}
        })

        for col in columns.values():
            name    = col["name"]
            dtype_s = col["dtype"].lower()
            kwargs  = {}

            # map source dtype → SQLAlchemy TypeEngine *instance*
            if dtype_s.startswith("int"):
                type_ = BigInteger()
            elif dtype_s.startswith("float"):
                # if decimals specified, use Numeric, else Float
                dec = col.get("decimals")
                if dec is not None:
                    type_ = Numeric(precision=16, scale=int(dec))
                else:
                    type_ = Float()
            elif dtype_s.startswith("string"):
                length = col.get("length")
                type_ = String(int(length)) if length else String(255)

            # pull through any other metadata
            if "nullable" in col:
                kwargs["nullable"] = bool(col["nullable"])
            if "default" in col:
                kwargs["default"] = col["default"]

            cols.append({
                "name": name,
                "type_": type_,
                "kwargs": kwargs
            })

        return cols

    def ensure_table(self, table_name, columns):
        inspector = inspect(self.engine)
        exists = inspector.has_table(table_name, schema=self.schema)

        # Step 1: create the table if it doesn’t exist at all
        metadata = MetaData(schema=self.schema)
        tbl = Table(table_name, metadata)
        for cfg in self.get_columns_config(columns):
            tbl.append_column(Column(cfg["name"], cfg["type_"], **cfg["kwargs"]))
        metadata.create_all(self.engine)

        # Step 2: if it did exist, only ADD the missing cols
        if exists:
            existing = Table(table_name, MetaData(),
                            schema=self.schema, autoload_with=self.engine)
            existing_cols = set(existing.columns.keys())

            with self.engine.begin() as conn:
                for cfg in self.get_columns_config(columns):
                    name = cfg["name"]
                    if name in existing_cols:
                        continue

                    # compile the SQL Server–specific type string
                    ddl_type = cfg["type_"].compile(dialect=self.engine.dialect)

                    # SQL Server wants ADD, not ADD COLUMN
                    stmt = text(
                        f'ALTER TABLE [{self.schema}].[{table_name}] '
                        f'ADD [{name}] {ddl_type}'
                    )
                    conn.execute(stmt)
        
    def upsert_via_merge(self, table_name: str, df: pd.DataFrame, pk_col: str = "TapahtumaId") -> None:
        """
        Perform an upsert (insert-or-update) of all rows in `df` into
        schema.table_name, matching on primary key column `pk_col`.
        """
        schema = self.schema
        engine = self.engine
        print(len(df))
        
        # 1) Make sure no NaNs sneak through
        df_clean = df.where(pd.notnull(df), None)

        # 2) Reflect the target table to get the full column list
        metadata = MetaData(schema=schema)
        target = Table(table_name, metadata, autoload_with=engine)
        all_cols = [c.name for c in target.columns]
        if pk_col not in all_cols:
            raise ValueError(f"PK column {pk_col!r} not in table {schema}.{table_name}")

        non_pk_cols = [c for c in all_cols if c != pk_col]

        # 3) Build the VALUES list and a flat params dict
        #    We'll name each parameter like :Asiakasnro_42, :Hinta_42, etc.
        value_rows = []
        params = {}
        for row_idx, record in enumerate(df_clean.to_dict(orient="records")):
            placeholders = []
            for col in all_cols:
                param_name = f"{col}_{row_idx}"
                placeholders.append(f":{param_name}")
                params[param_name] = record.get(col)
            value_rows.append(f"({', '.join(placeholders)})")

        values_clause = ",\n  ".join(value_rows)

        # 4) Compose the MERGE statement
        all_cols_quoted = ", ".join(f"[{c}]" for c in all_cols)
        non_pk_set = ",\n    ".join(
            f"target.[{c}] = source.[{c}]" for c in non_pk_cols
        )
        insert_cols   = all_cols_quoted
        insert_vals   = ", ".join(f"source.[{c}]" for c in all_cols)

        merge_sql = f"""
            MERGE INTO [{schema}].[{table_name}] AS target
            USING (
            VALUES
            {values_clause}
            ) AS source ({all_cols_quoted})
            ON target.[{pk_col}] = source.[{pk_col}]
            WHEN MATCHED THEN
            UPDATE SET
                {non_pk_set}
            WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
        """

        # 5) Execute it
        with engine.begin() as conn:
            conn.execute(text(merge_sql), params)
            
    def upsert_with_staging(self, table_name, df, pk_col="TapahtumaId"):
        staging = f"{table_name}_stg"
        schema = self.schema
        engine = self.engine

        # 1) Load staging table in one shot
        df_clean = df.where(pd.notnull(df), None)
        df_clean.to_sql(
            name=staging,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method=None,
            chunksize=2000
        )

        # 2) Build MERGE statement (reflect column list)
        target = f"[{schema}].[{table_name}]"
        src    = f"[{schema}].[{staging}]"
        # grab columns dynamically:
        meta   = MetaData(schema=schema)
        tbl    = Table(table_name, meta, autoload_with=engine)
        all_cols   = [c.name for c in tbl.columns]
        non_pk_cols = [c for c in all_cols if c != pk_col]

        update_set = ",\n    ".join(
            f"t.[{c}] = s.[{c}]" for c in non_pk_cols
        )
        cols_list  = ", ".join(f"[{c}]" for c in all_cols)
        src_cols   = ", ".join(f"s.[{c}]" for c in all_cols)

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

        # 3) Execute and clean up
        with engine.begin() as conn:
            conn.execute(text(merge_sql))
            conn.execute(text(f"DROP TABLE {schema}.{staging}"))

    def fetch_dataframe(self, table_name: str) -> pd.DataFrame:
        """
        Fetch all rows from the specified table as a DataFrame.
        """
        
        df = pd.read_sql(
            sql=f"SELECT * FROM [{self.schema}].[{table_name}]",
            con=self.engine
        )
        return df

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
        local_test: bool = False
    ) -> None:
        if self._initialized:
            return
        if driver is None:
            server = os.getenv("SQL_SERVER")
            database = os.getenv("SQL_DATABASE")
            odbc_driver = "{ODBC Driver 18 for SQL Server}"
            driver = _AzureDriver(
                server=server, 
                database=database, 
                driver=odbc_driver, 
                local_test=local_test)
        self.driver = driver
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
        self.driver.upsert_with_staging(table_name=table, df=df)
        
    def fetch_dataframe(self, customer: str) -> pd.DataFrame:
        table = self._sanitize(customer)
        return self.driver.fetch_dataframe(table_name=table)


