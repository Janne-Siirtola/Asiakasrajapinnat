"""Data cleaning and validation helpers for customer exports."""

from itertools import chain
import logging
import numpy as np
import pandas as pd

from .customer import Customer


class DataEditor:
    """Utility class for cleaning and validating exported data."""

    def __init__(self, df: pd.DataFrame, customer: Customer):
        self.df = df.copy()
        self.customer = customer

        self.target_row_count = len(self.df) - 1

        self.mappings = customer.mappings

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
        to_drop = set(self.df.columns) - \
            set(self.mappings.allowed_columns.keys())

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

    def rename_columns(self) -> "DataEditor":
        """Rename columns according to the mapping."""
        self.df = self.df.rename(columns=self.mappings.rename_map)
        return self

    def cast_datatypes(self) -> "DataEditor":
        """
        Cast the DataFrame columns to their specified types and round them if necessary.
        """

        valid_dtypes = {
            col: dt
            for col, dt in self.mappings.dtype_map.items()
            if col in self.df.columns
        }

        for col, dt in valid_dtypes.items():
            if col not in self.df.columns:
                continue

            series = self.df[col]

            if dt.startswith('float'):
                # normalize decimal separator
                series = series.astype(str).str.replace(',', '.', regex=False)
                self.df[col] = series.astype(float)
            elif dt.startswith('int'):
                self.df[col] = series.astype(int)

        return self
    
    def calculate_esrs(self) -> "DataEditor":
        """
        Calculate the ESRs (European Single Procurement Document) for the DataFrame.
        This is a placeholder for actual ESR calculation logic.
        """
        
        df = self.df
        mat_hyotyaste = df['Materiaalihyotyaste']
        ene_hyotyaste = df['Energiahyotyaste']
        paino = df['Paino']
        jatetyyppi = df['JateTyyppi']
        
        add_to_allowed = []
        
        a1 = '37a_hyodyntaminen'
        df[a1] = (
            ((mat_hyotyaste * paino) + (ene_hyotyaste * paino)) / 100
        ).where((jatetyyppi.isin([0, 1])) & (paino >= 0), 0)  # If JateTyyppi is 1, use the formula
        add_to_allowed.append(a1)

        a2 = '37a_loppukasittely'
        df[a2] = (
            (mat_hyotyaste * paino) + (ene_hyotyaste * paino) / 100
        ).where((jatetyyppi == 2) & (paino >= 0), 0) # VARMISTA TÄMÄ OLLILTA
        add_to_allowed.append(a2)

        a_yht = '37a_yhteensa'
        df[a_yht] = (
            df[a1] + df[a2]
        )
        add_to_allowed.append(a_yht)

        # ------- 37b -------
        
        b1 = '37b_valmistelu_uudelleenkäyttöön'
        df[b1] = 0 # Placeholder for actual calculation
        add_to_allowed.append(b1)

        b2 = '37b_kierratys'
        df[b2] = (
            mat_hyotyaste * paino / 100
        ).where (paino >= 0, 0)
        add_to_allowed.append(b2)

        b3 = '37b_muut_hyodyntamistoimet'
        df[b3] = (
            ene_hyotyaste * paino / 100
        ).where (paino >= 0, 0)
        add_to_allowed.append(b3)

        b_yht = '37b_yhteensa'
        df[b_yht] = (
            df[b1] +
            df[b2] +
            df[b3]
        )
        add_to_allowed.append(b_yht)

        # -------- 37c -------
        c1 = '37c_poltto'
        df[c1] = 0
        add_to_allowed.append(c1)
        
        c2 = '37c_kaatopaikka'
        df[c2] = 0
        add_to_allowed.append(c2)
        
        c3 = '37c_muu_loppukasittely'
        df[c3] = 0
        add_to_allowed.append(c3)
        
        c_yht = '37c_yhteensa'
        df[c_yht] = (
            df[c1] +
            df[c2] +
            df[c3]
        )
        add_to_allowed.append(c_yht)

        # -------- 37d -------
        d1 = '37d_kokonaismaara'
        df[d1] = (
            df[b3] + df[c_yht]
        )
        add_to_allowed.append(d1)
        
        d2 = '37d_osuus'
        df[d2] = (
            df[d1] / df[a_yht]
        ).where(df[a_yht] > 0, 0)  # Avoid division by zero
        add_to_allowed.append(d2)

        for col in add_to_allowed:
            if col not in self.mappings.allowed_columns:
                self.mappings.allowed_columns[col] = col
        
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

        if self.df.empty:
            error_logs.append("DataFrame is empty after processing")

        # Check for extra columns
        extras = set(self.df.columns) - \
            set(self.mappings.allowed_columns.values())
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
                "Customer: %s\n%s", self.customer.config.name, "\n".join(
                    warning_logs)
            )

        return self
