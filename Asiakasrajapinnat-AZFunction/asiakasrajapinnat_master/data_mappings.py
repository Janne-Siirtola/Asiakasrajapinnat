"""DataEditor mapping configuration."""

from dataclasses import dataclass, field
from typing import Dict, Union


@dataclass
class DataMappings:
    """Container for DataEditor mapping configuration."""

    rename_map: Dict[str, str] = field(default_factory=dict)
    dtype_map: Dict[str, str] = field(default_factory=dict)
    decimals_map: Dict[str, int] = field(default_factory=dict)
    combined_columns: Dict[str, Dict[str, Union[str, int]]] = field(
        default_factory=dict)
    allowed_columns: Dict[str, str] = field(default_factory=dict)
