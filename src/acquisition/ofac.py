# src/acquisition/ofac.py
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from loguru import logger
from .base import BaseAcquirer

OFAC_SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.xml"

class OFACAcquirer(BaseAcquirer):

    def fetch(self, **kwargs) -> Path:
        logger.info("Downloading OFAC SDN XML...")
        resp = requests.get(OFAC_SDN_URL, timeout=120)
        resp.raise_for_status()

        out = self.output_dir / "ofac_sdn.xml"
        out.write_bytes(resp.content)
        logger.info(f"Saved OFAC SDN XML ({len(resp.content)//1024} KB) -> {out}")
        return out

    def to_staging(self, raw_path: Path) -> pd.DataFrame:
        logger.info("Parsing OFAC SDN XML...")
        tree = ET.parse(raw_path)
        root = tree.getroot()

        records = []
        for entry in root.iter():
            if not entry.tag.endswith("sdnEntry"):
                continue

            uid      = entry.findtext(".//{*}uid") or ""
            name     = entry.findtext(".//{*}lastName") or ""
            sdn_type = entry.findtext(".//{*}sdnType") or ""
            program  = entry.findtext(".//{*}program") or ""
            country  = entry.findtext(".//{*}country") or ""

            if not name.strip():
                continue

            aliases = [
                aka.findtext(".//{*}lastName") or ""
                for aka in entry.findall(".//{*}aka")
                if aka.findtext(".//{*}lastName")
            ]

            entity_type = "COMPANY" if sdn_type == "Entity" else "PERSON"

            records.append({
                "raw_id":       f"ofac_{uid}",
                "surface_form": name.strip(),
                "entity_type":  entity_type,
                "source":       "ofac",
                "date":         "",
                "confidence":   1.0,
                "metadata": {
                    "program":  program,
                    "country":  country,
                    "aliases":  aliases,
                    "sdn_type": sdn_type,
                },
            })

        df = pd.DataFrame(records)
        self.validate_staging(df)

        out = self.output_dir / "ofac_staging.parquet"
        df.to_parquet(out)
        logger.info(f"Staged {len(df)} OFAC entities -> {out}")
        return df