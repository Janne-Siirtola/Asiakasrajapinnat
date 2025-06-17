"""Customer configuration model and data helpers."""

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
import json
from pathlib import Path
from typing import Optional, Dict, Union, Set
import os
# from azure.storage.blob import BlobServiceClient
import numpy as np
import pandas as pd
import logging
import io
from .storage_handler import StorageHandler


class Customer:
    def __init__(
        self,
        name: str,
        konserni: Set[int],
        source_container: str,
        destination_container: str,
        file_format: str,
        file_encoding: str,
        extra_columns: Optional[Dict[str, Dict[str, str]]],
        enabled: bool,
        base_columns: Dict[str, Dict[str, str]],
        exclude_columns: Optional[list[str]] = None,
    ) -> None:
        """Store the customer configuration used during processing."""

        self.name = name
        self.konserni = konserni
        self.source_container = source_container
        self.destination_container = destination_container
        self.file_format = file_format
        self.file_encoding = file_encoding
        self.extra_columns = extra_columns
        self.enabled = enabled

        self.base_columns = base_columns
        self.exclude_columns = exclude_columns if exclude_columns else []

        if self.exclude_columns:
            for c in self.exclude_columns:
                self.base_columns.pop(c)

        self.rename_map: Dict[str, str] = {}
        self.dtype_map: Dict[str, str] = {}
        self.decimals_map: Dict[str, int] = {}
        self.combined_columns: Dict[str, Dict[str, Union[str, int]]] = {}

        self.generate_combined_columns()
        self.generate_data_maps()

    def get_data(self, stg: StorageHandler, stg_prefix: Optional[str] = None) -> pd.DataFrame:
        """Load the newest CSV file from the customer's source container."""
        # 0) normalize and build our two “directories”
        prefix = stg_prefix.rstrip('/') + '/'
        history_dir = prefix + 'history/'

        # 1) list just the CSVs directly under `prefix`
        all_blobs = stg.container_client.list_blobs(name_starts_with=prefix)
        csv_blobs = [
            b for b in all_blobs
            if b.name.lower().endswith('.csv')
               and '/' not in b.name[len(prefix):]  # no extra “subfolder”
        ]

        if not csv_blobs:
            return pd.DataFrame()   # empty df so df.empty == True

        # 2) pick the latest
        latest = max(csv_blobs, key=lambda b: b.last_modified)

        # 3) download it
        data = stg.download_blob(latest.name)
        logging.info(
            f"Loaded {len(data)} bytes from {stg.container_name}/{latest.name} "
            f"(last modified: {latest.last_modified})"
        )

        # 4) move it into history
        stg.move_file_to_dir(
            latest.name, target_dir=history_dir, overwrite=True)

        # 5) parse and return
        df = pd.read_csv(io.BytesIO(data),
                         encoding='ISO-8859-1',
                         delimiter=';')
        return df

    def generate_combined_columns(self) -> None:
        """
        Generate a dictionary of allowed columns based on the customer's base_columns and extra_columns.
        :return: Dictionary of allowed columns.
        """
        for key, value in self.base_columns.items():
            if key not in self.extra_columns:
                self.combined_columns[key] = value
            else:
                print(
                    f"Duplicate key '{key}' found in base_columns, skipping.")

        for key, value in self.extra_columns.items():
            if key not in self.combined_columns:
                self.combined_columns[key] = value
            else:
                print(
                    f"Duplicate key '{key}' found in extra_columns, skipping.")

    def generate_data_maps(self) -> None:
        """Create rename, dtype and decimals mappings for processing."""
        # 1) rename mapping: old_key → new_name
        self.rename_map = {old: cfg["name"]
                           for old, cfg in self.combined_columns.items()}

        # 2) dtype mapping: new_name → dtype
        self.dtype_map = {cfg["name"]: cfg["dtype"]
                          for cfg in self.combined_columns.values()}

        # 3) decimals mapping (only those that specify decimals)
        self.decimals_map = {cfg["name"]: cfg["decimals"]
                             for cfg in self.combined_columns.values()
                             if "decimals" in cfg}

        self.allowed_columns = self.rename_map.copy()
