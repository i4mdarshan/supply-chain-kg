# src/acquisition/base.py
from abc import ABC, abstractmethod
from pathlib import Path
from loguru import logger
import pandas as pd

class BaseAcquirer(ABC):
    """All data sources implement this interface."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def fetch(self, **kwargs) -> Path:
        """Download raw data, return path to saved file."""
        ...

    @abstractmethod
    def to_staging(self, raw_path: Path) -> pd.DataFrame:
        """Parse raw file into unified staging schema."""
        ...

    @property
    def staging_schema(self) -> list[str]:
        return [
            "raw_id",        # source-specific ID
            "surface_form",  # extracted entity name
            "entity_type",   # COMPANY | COUNTRY | PORT etc.
            "source",        # 'gdelt' | 'ofac' | 'eu_sanctions'
            "date",          # ISO date string
            "confidence",    # float 0–1
            "metadata",      # dict — source-specific extras
        ]

    def validate_staging(self, df: pd.DataFrame) -> bool:
        missing = set(self.staging_schema) - set(df.columns)
        if missing:
            logger.error(f"{self.__class__.__name__}: missing columns {missing}")
            return False
        logger.info(f"{self.__class__.__name__}: {len(df)} records staged")
        return True