
"""Load the global configuration used by the timer function."""

import json
from dataclasses import dataclass
from typing import Dict

from .storage_handler import StorageHandler


@dataclass
class MainConfig:
    """Configuration values shared between all customers."""

    base_columns: Dict[str, Dict[str, str]]


def load_main_config(conf_stg: StorageHandler) -> MainConfig:
    """Load ``MainConfig.json`` from storage and return the configuration."""
    json_data = conf_stg.download_blob("MainConfig.json")
    if not json_data:
        raise ValueError(
            "MainConfig.json is empty or not found in the storage.")
    raw = json.loads(json_data)
    return MainConfig(base_columns=raw.get("base_columns", {}))
