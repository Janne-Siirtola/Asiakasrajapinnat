
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass
class MainConfig:
    customer_config_path: Path
    src_container_prefix: str
    base_columns: Dict[str, Dict[str, str]]