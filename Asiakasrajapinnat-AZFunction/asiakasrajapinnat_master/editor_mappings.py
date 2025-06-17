from dataclasses import dataclass
from typing import Dict, Union


@dataclass
class EditorMappings:
    """Container for DataEditor mapping configuration."""

    rename_map: Dict[str, str]
    dtype_map: Dict[str, str]
    decimals_map: Dict[str, int]
    combined_columns: Dict[str, Dict[str, Union[str, int]]]
    allowed_columns: Dict[str, str]
