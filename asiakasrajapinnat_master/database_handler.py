"""SQLite database handler for storing customer data."""

from __future__ import annotations

import os
import sqlite3
from typing import Dict, Iterable

import pandas as pd


class DatabaseHandler:
    """Singleton wrapper around a SQLite database."""

    _instance: "DatabaseHandler | None" = None

    def __new__(cls, db_path: str = "customer_data.db", base_columns: Dict[str, Dict[str, str]] | None = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_path: str = "customer_data.db", base_columns: Dict[str, Dict[str, str]] | None = None) -> None:
        if self._initialized:
            return
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.base_columns = base_columns or {}
        self._initialized = True

    # -- internal helpers -------------------------------------------------
    @staticmethod
    def _sanitize(name: str) -> str:
        return name.replace("-", "_").replace(" ", "_")

    @staticmethod
    def _sqlite_type(dtype: str) -> str:
        if dtype.startswith("int"):
            return "INTEGER"
        if dtype.startswith("float"):
            return "REAL"
        return "TEXT"

    def _table_columns(self, table: str) -> set[str]:
        cur = self.conn.execute(f"PRAGMA table_info('{table}')")
        return {row[1] for row in cur.fetchall()}

    # -- public API -------------------------------------------------------
    def ensure_table(self, customer: str, base_columns: Dict[str, Dict[str, str]] | None = None) -> None:
        if base_columns is not None:
            self.base_columns.update(base_columns)
        columns = self.base_columns
        table = self._sanitize(customer)
        existing = self._table_columns(table)
        if not existing:
            cols_sql = ["'TapahtumaId' TEXT PRIMARY KEY"]
            for cfg in columns.values():
                cols_sql.append(f"'{cfg['name']}' {self._sqlite_type(cfg['dtype'])}")
            sql = f"CREATE TABLE IF NOT EXISTS '{table}' ({', '.join(cols_sql)})"
            self.conn.execute(sql)
            self.conn.commit()
            existing = self._table_columns(table)

        for cfg in columns.values():
            col = cfg["name"]
            if col not in existing:
                sql = f"ALTER TABLE '{table}' ADD COLUMN '{col}' {self._sqlite_type(cfg['dtype'])}"
                self.conn.execute(sql)
        if 'TapahtumaId' not in existing:
            self.conn.execute(f"ALTER TABLE '{table}' ADD COLUMN 'TapahtumaId' TEXT")
        self.conn.commit()

    def upsert_rows(self, customer: str, df: pd.DataFrame) -> None:
        table = self._sanitize(customer)
        self.ensure_table(customer)
        if 'TapahtumaId' not in df.columns:
            raise KeyError('TapahtumaId column missing from data')
        columns = [c["name"] for c in self.base_columns.values() if c["name"] in df.columns]
        all_cols = ['TapahtumaId'] + columns
        placeholders = ','.join('?' for _ in all_cols)
        assignments = ','.join(f"'{c}'=excluded.'{c}'" for c in all_cols if c != 'TapahtumaId')
        sql = (
            f"INSERT INTO '{table}' ({','.join(all_cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT(TapahtumaId) DO UPDATE SET {assignments}"
        )
        self.conn.executemany(sql, df[all_cols].itertuples(index=False, name=None))
        self.conn.commit()

    def fetch_dataframe(self, customer: str) -> pd.DataFrame:
        table = self._sanitize(customer)
        self.ensure_table(customer)
        cur = self.conn.execute(f"SELECT * FROM '{table}'")
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame(columns=['TapahtumaId'] + [cfg['name'] for cfg in self.base_columns.values()])
        df = pd.DataFrame(rows, columns=rows[0].keys())
        return df
