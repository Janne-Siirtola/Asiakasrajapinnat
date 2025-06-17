
"""Load the global configuration used by the timer function."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict
import json
from .StorageHandler import StorageHandler


class MainConfig:
    """Configuration values shared between all customers."""

    def __init__(self, conf_stg: StorageHandler):
        """Load ``MainConfig.json`` from storage and parse it."""
        json_data = conf_stg.download_blob("MainConfig.json")
        if not json_data:
            raise ValueError("MainConfig.json is empty or not found in the storage.")
        raw = json.loads(json_data)
        
        self.initialize_config(**raw)
        
        
    def initialize_config(self, customer_config_path: Path, src_container_prefix: str, base_columns: Dict[str, Dict[str, str]]) -> None:
        """Set up configuration attributes after loading the JSON file."""
        self.customer_config_path = customer_config_path
        self.src_container_prefix = src_container_prefix
        self.base_columns = base_columns