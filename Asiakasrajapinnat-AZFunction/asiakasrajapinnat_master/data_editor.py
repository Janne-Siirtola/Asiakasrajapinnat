"""Data cleaning and validation helpers for customer exports."""

import logging
import pandas as pd

from .customer import Customer
from .editor_mappings import EditorMappings


class DataEditor:
    """Utility class for cleaning and validating exported data."""

    def __init__(self, df: pd.DataFrame, customer: Customer):
        self.df = df.copy()
        self.customer = customer

        self.target_row_count = len(self.df) - 1

        self.mappings = EditorMappings(
            rename_map=customer.mappings.rename_map,
            dtype_map=customer.mappings.dtype_map,
            decimals_map=customer.mappings.decimals_map,
            combined_columns=customer.mappings.combined_columns,
            allowed_columns=customer.mappings.allowed_columns,
        )

        self.total_weight_before = self.df['TAPPaino'].sum()

    def delete_row(self, idx: int) -> "DataEditor":
        """Remove a row by index from the working DataFrame."""
        self.df = self.df.drop(idx).reset_index(drop=True)
        return self

    def validate_concern_number(self) -> "DataEditor":
        """
        Validation step: ensure every non‐blank PARConcern value
        (internal column name) is in customer.konserni.
        If any value fails, abort with an error.
        """
        col = "PARConcern"
        if col not in self.df.columns:
            raise KeyError(f"Expected konserni-column '{col}' not found")

        allowed = {str(i) for i in self.customer.config.konserni}
        unique_vals = set(self.df[col])

        if not unique_vals.issubset(allowed):
            extra = unique_vals - allowed
            raise ValueError(
                f"Invalid konserni values found: {extra}")

        # all good, return self unchanged
        return self

    def drop_unmapped_columns(self) -> "DataEditor":
        """Remove columns that are not defined in the mapping."""
        to_drop = set(self.df.columns) - set(self.mappings.allowed_columns.keys())

        self.df = self.df.drop(columns=to_drop)

        if to_drop:
            logging.info("Dropping unmapped columns: %s", to_drop)

        return self

    def reorder_columns(self) -> "DataEditor":
        """Order columns according to the allowed mapping."""
        ordered = [c for c in self.mappings.allowed_columns.keys()
                   if c in self.df.columns]
        self.df = self.df[ordered]
        return self

    def cast_and_round(self) -> "DataEditor":
        """
        Cast the DataFrame columns to their specified types and round them if necessary.
        """
        # 1) rename
        self.df = self.df.rename(columns=self.mappings.rename_map)

        # 2) cast datatypes
        valid_dtypes = {
            col: dt
            for col, dt in self.mappings.dtype_map.items()
            if col in self.df.columns
        }

        for col, dt in valid_dtypes.items():
            if col not in self.df.columns:
                continue

            series = self.df[col]

            if dt.startswith('float') or dt in ('Float64',):
                # normalize decimal separator
                series = series.astype(str).str.replace(',', '.', regex=False)
                self.df[col] = series.astype(float)

        return self

    def validate_final_df(self) -> "DataEditor":
        """
        Validate the final DataFrame.
        """
        warning_logs = []
        error_logs = []

        # Check for missing columns
        missing = [col for col in self.mappings.allowed_columns.values()
                   if col not in self.df.columns]
        if missing:
            warning_logs.append(
                f"These base columns were not found in the DataFrame: {missing}"
            )

        if self.total_weight_before != self.df['TAPPaino'].sum():
            error_logs.append(
                f"Total weight mismatch: before {self.total_weight_before}, "
                f"after {self.df['TAPPaino'].sum()}"
            )

        if self.df.empty:
            error_logs.append("DataFrame is empty after processing")

        # Check for extra columns
        extras = set(self.df.columns) - set(self.mappings.allowed_columns.values())
        if extras:
            error_logs.append(
                f"Unexpected extra columns in final DataFrame: {sorted(extras)}")

        # Check for duplicate column names
        if len(self.df.columns) != len(self.df.columns.unique()):
            error_logs.append(
                "Duplicate column names detected in final DataFrame")

        # Check the dataframe row count
        current_row_count = len(self.df)
        if current_row_count != self.target_row_count:
            error_logs.append(
                f"Row count mismatch: expected {self.target_row_count}, got {current_row_count}"
            )

        # Check for index integrity
        if not self.df.index.is_unique:
            error_logs.append("DataFrame index contains duplicates")
        if list(self.df.index) != list(range(len(self.df))):
            error_logs.append(
                "DataFrame index is not a simple RangeIndex 0…n-1")

        if error_logs:
            raise ValueError(
                "Customer: %s\n%s" % (self.customer.config.name,
                                      "\n".join(error_logs))
            )

        if warning_logs:
            logging.warning(
                "Customer: %s\n%s", self.customer.config.name, "\n".join(warning_logs)
            )

        return self
