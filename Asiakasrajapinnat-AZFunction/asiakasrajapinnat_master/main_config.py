
"""Load the global configuration used by the timer function."""

import json

from .storage_handler import StorageHandler


class MainConfig:
    """Configuration values shared between all customers."""

    def __init__(self, conf_stg: StorageHandler):
        """Load ``MainConfig.json`` from storage and parse it."""
        json_data = conf_stg.download_blob("MainConfig.json")
        if not json_data:
            raise ValueError(
                "MainConfig.json is empty or not found in the storage.")
        raw = json.loads(json_data)

        self.base_columns = raw.get("base_columns", {})
