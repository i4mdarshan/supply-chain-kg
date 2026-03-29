# src/extraction/sanctions_cleaner.py
import pandas as pd
import re
from loguru import logger

# Common legal suffixes to normalize
LEGAL_SUFFIXES = [
    r"\bCO\.?,?\s*LTD\.?\b",
    r"\bCORP\.?\b",
    r"\bINC\.?\b",
    r"\bLLC\.?\b",
    r"\bL\.L\.C\.?\b",
    r"\bPLC\.?\b",
    r"\bGMBH\.?\b",
    r"\bS\.A\.?\b",
    r"\bLTD\.?\b",
]

class SanctionsCleaner:

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize surface forms for OFAC and EU sanctions rows.
        - Strips extra whitespace
        - Title-cases all-caps names
        - Removes special characters that break entity matching
        """
        sanctions = df["source"].isin(["ofac", "eu_sanctions"])
        cleaned   = df.copy()

        cleaned.loc[sanctions, "surface_form"] = (
            cleaned.loc[sanctions, "surface_form"]
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)      # collapse whitespace
            .str.replace(r"[^\w\s\-\.\,\(\)\'&]", "", regex=True)  # remove noise chars
            .apply(self._normalize_case)
        )

        logger.info(
            f"Cleaned {sanctions.sum()} sanctions rows "
            f"({df[sanctions]['source'].value_counts().to_dict()})"
        )
        return cleaned

    def _normalize_case(self, name: str) -> str:
        """Title-case if ALL CAPS, leave mixed case alone."""
        if not isinstance(name, str):
            return name
        if name.isupper():
            return name.title()
        return name