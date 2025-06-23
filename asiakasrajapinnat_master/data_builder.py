"""Helpers for building output files for each customer."""

import json
from typing import Any, Hashable
import numpy as np
import pandas as pd
from .customer import Customer


class DataBuilder:
    """
    A class to build a JSON object from a DataFrame.
    """

    def __init__(self, customer: Customer):
        self.decimals_map = customer.mappings.decimals_map

    def format_json_row(self, row: dict[Hashable, Any]) -> str:
        """Convert a pandas row to compact JSON without extra spaces."""
        parts = []
        for col, val in row.items():
            # key as JSON string
            key = json.dumps(col, ensure_ascii=False)
            if col in self.decimals_map and pd.notna(val):
                # numeric with fixed decimals
                fmt = f"{{:.{self.decimals_map[col]}f}}"
                num = fmt.format(val)
                parts.append(f"{key}:{num}")
            else:
                # dump everything else normally (strings, ints, None, etc.)
                parts.append(f"{key}:{json.dumps(val, ensure_ascii=False)}")
        return "{" + ",".join(parts) + "}"

    def build_json(self, df_final: pd.DataFrame) -> str:
        """Return the dataframe as a JSON string."""
        json_data = "["
        rows = df_final.to_dict(orient="records")
        for i, row in enumerate(rows):
            json_data += self.format_json_row(row) + "\n"
            if i < len(rows) - 1:
                json_data += ","
        json_data += "]"

        return json_data

    def build_csv(self, df_final: pd.DataFrame, encoding: str) -> str:
        """Return the dataframe in CSV format using the given encoding."""
        return df_final.to_csv(index=False, encoding=encoding, sep=";", decimal=".")
