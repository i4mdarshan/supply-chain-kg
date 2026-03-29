# src/acquisition/eu_sanctions.py
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from loguru import logger
from .base import BaseAcquirer

EU_SANCTIONS_URL = (
    "https://webgate.ec.europa.eu/fsd/fsf/public/files/"
    "xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw"
)

class EUSanctionsAcquirer(BaseAcquirer):

    def fetch(self, **kwargs) -> Path:
        logger.info("Downloading EU Sanctions XML...")
        resp = requests.get(
            EU_SANCTIONS_URL,
            timeout=120,
            headers={"User-Agent": "Mozilla/5.0 (compatible; research-project/1.0)"}
        )
        resp.raise_for_status()

        out = self.output_dir / "eu_sanctions.xml"
        out.write_bytes(resp.content)
        logger.info(f"Saved EU Sanctions XML ({len(resp.content)//1024} KB) -> {out}")
        return out

    def to_staging(self, raw_path: Path) -> pd.DataFrame:
        logger.info("Parsing EU Sanctions XML...")
        tree = ET.parse(raw_path)
        root = tree.getroot()

        NS = "http://eu.europa.ec/fpi/fsd/export"
        logger.info(f"Root tag: {root.tag}")

        records = []
        for entry in root.findall(f"{{{NS}}}sanctionEntity"):

            logical_id     = entry.get("logicalId", "")
            eu_ref         = entry.get("euReferenceNumber", "")

            # Subject type — classificationCode: P=Person, E=Entity
            type_node      = entry.find(f"{{{NS}}}subjectType")
            class_code     = type_node.get("classificationCode", "") if type_node is not None else ""
            entity_type    = "COMPANY" if class_code == "E" else "PERSON"

            # Regulation — all fields are attributes
            reg_node       = entry.find(f"{{{NS}}}regulation")
            programme      = reg_node.get("programme", "")      if reg_node is not None else ""
            pub_date       = reg_node.get("publicationDate", "") if reg_node is not None else ""
            number_title   = reg_node.get("numberTitle", "")    if reg_node is not None else ""

            # Name aliases — wholeName + firstName/lastName are attributes
            aliases        = entry.findall(f"{{{NS}}}nameAlias")
            if not aliases:
                continue

            # Primary name = first strong alias, fallback to first alias
            primary        = next(
                (a for a in aliases if a.get("strong") == "true"),
                aliases[0]
            )
            name           = primary.get("wholeName", "").strip()
            if not name:
                first = primary.get("firstName", "")
                last  = primary.get("lastName", "")
                name  = f"{first} {last}".strip()
            if not name:
                continue

            # All other alias names
            alias_names    = [
                a.get("wholeName", "").strip()
                for a in aliases
                if a.get("wholeName", "").strip() and a.get("wholeName", "").strip() != name
            ]

            # Citizenship country
            citizen_node   = entry.find(f"{{{NS}}}citizenship")
            country        = citizen_node.get("countryIso2Code", "") if citizen_node is not None else ""

            records.append({
                "raw_id":       f"eu_{logical_id}",
                "surface_form": name,
                "entity_type":  entity_type,
                "source":       "eu_sanctions",
                "date":         pub_date,
                "confidence":   1.0,
                "metadata": {
                    "eu_ref":       eu_ref,
                    "programme":    programme,
                    "regulation":   number_title,
                    "country":      country,
                    "class_code":   class_code,
                    "aliases":      alias_names,
                },
            })

        df = pd.DataFrame(records)
        self.validate_staging(df)

        out = self.output_dir / "eu_sanctions_staging.parquet"
        df.to_parquet(out)
        logger.info(f"Staged {len(df)} EU sanctioned entities -> {out}")
        return df