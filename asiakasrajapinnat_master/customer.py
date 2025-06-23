"""Customer configuration model and data helpers."""

import io
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Set

import pandas as pd
from .storage_handler import StorageHandler
from .data_mappings import DataMappings


@dataclass
class CustomerConfig:
    """Configuration values for a single customer."""
    name: str
    konserni: Set[int]
    source_container: str
    destination_container: str
    file_format: str
    file_encoding: str
    extra_columns: Optional[Dict[str, Dict[str, str]]]
    enabled: bool
    base_columns: Dict[str, Dict[str, str]]
    exclude_columns: Optional[list[str]] = None


class Customer:
    """Configuration data and helpers for an individual customer."""

    def __init__(self, config: CustomerConfig) -> None:
        """Store the customer configuration used during processing."""

        self.config = config

        self.base_columns = config.base_columns.copy()
        self.exclude_columns = config.exclude_columns or []

        if self.exclude_columns:
            logging.info(
                "Excluding from base columns: %s", self.exclude_columns)
            for c in self.exclude_columns:
                self.base_columns.pop(c)

        self.mappings = DataMappings()

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
            "Loaded %d bytes from %s/%s (last modified: %s)",
            len(data),
            stg.container_name,
            latest.name,
            latest.last_modified,
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
        Generate a dictionary of allowed columns based on the customer's
        base_columns and extra_columns.
        :return: Dictionary of allowed columns.
        """
        extra = self.config.extra_columns or {}
        for key, value in self.base_columns.items():
            if key not in extra:
                self.mappings.combined_columns[key] = value
            else:
                logging.info(
                    "Duplicate key '%s' found in base_columns, skipping.", key)

        for key, value in extra.items():
            if key not in self.mappings.combined_columns:
                self.mappings.combined_columns[key] = value
            else:
                logging.info(
                    "Duplicate key '%s' found in extra_columns, skipping.", key)

    def generate_data_maps(self) -> None:
        """Create rename, dtype and decimals mappings for processing."""
        # 1) rename mapping: old_key → new_name
        self.mappings.rename_map = {
            old: cfg["name"] for old, cfg in self.mappings.combined_columns.items()
        }

        # 2) dtype mapping: new_name → dtype
        self.mappings.dtype_map = {
            cfg["name"]: cfg["dtype"] for cfg in self.mappings.combined_columns.values()
        }

        # 3) decimals mapping (only those that specify decimals)
        self.mappings.decimals_map = {
            cfg["name"]: cfg["decimals"]
            for cfg in self.mappings.combined_columns.values()
            if "decimals" in cfg
        }

        self.mappings.allowed_columns = self.mappings.rename_map.copy()
