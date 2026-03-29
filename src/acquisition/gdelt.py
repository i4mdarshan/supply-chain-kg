# src/acquisition/gdelt.py
from pathlib import Path
from datetime import date, timedelta
import zipfile, io, requests
import pandas as pd
from loguru import logger
from .base import BaseAcquirer

# GDELT GKG 2.0 bulk download — free, no auth needed
GDELT_GKG_INDEX = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
GDELT_GKG_BASE  = "http://data.gdeltproject.org/gdeltv2/"

# Supply chain CAMEO theme filters
SC_THEMES = [
    "SUPPLY_CHAIN", "SANCTION", "TRADE_DISPUTE",
    "PORT_CLOSURE", "TRANSPORTATION", "EMBARGO",
    "ECON_TRADE_DISPUTE", "ENV_DEFORESTATION",
]

class GDELTAcquirer(BaseAcquirer):

    def fetch(self, start: date, end: date, max_files: int = 50) -> Path:
        """
        Download GDELT GKG CSV files for a date range.
        Each 15-min file is ~2-5MB zipped.
        max_files=50 gives ~5,000-10,000 events.
        """
        logger.info(f"Fetching GDELT GKG: {start} -> {end}, max {max_files} files")
        index = self._fetch_index()
        gkg_urls = [u for u in index if ".gkg.csv.zip" in u]

        # Filter to date range
        in_range = [u for u in gkg_urls if self._url_in_range(u, start, end)]
        selected = in_range[:max_files]
        logger.info(f"Downloading {len(selected)} GKG files")

        all_frames = []
        for url in selected:
            try:
                df = self._download_gkg_file(url)
                all_frames.append(df)
            except Exception as e:
                logger.warning(f"Skipped {url}: {e}")

        combined = pd.concat(all_frames, ignore_index=True)
        out = self.output_dir / f"gdelt_raw_{start}_{end}.parquet"
        combined.to_parquet(out)
        logger.info(f"Saved {len(combined)} raw GKG records -> {out}")
        return out

    def to_staging(self, raw_path: Path) -> pd.DataFrame:
        df = pd.read_parquet(raw_path)

        # Filter to supply-chain-relevant themes
        sc_mask = df["Themes"].fillna("").str.contains(
            "|".join(SC_THEMES), case=False
        )
        filtered = df[sc_mask].copy()
        filtered = filtered[filtered["Organizations"].fillna("").str.strip() != ""]
        logger.info(f"Theme filter: {len(df)} -> {len(filtered)} SC-relevant records")

        staged = pd.DataFrame({
            "raw_id":       filtered["GKGRECORDID"],
            "surface_form": filtered["Persons"].fillna("") + " " +
                            filtered["Organizations"].fillna(""),
            "entity_type":  "DISRUPTION_EVENT",
            "source":       "gdelt",
            "date":         pd.to_datetime(
                                filtered["DATE"].astype(str),
                                format="%Y%m%d%H%M%S", errors="coerce"
                            ).dt.date.astype(str),
            "confidence":   0.7,
            "metadata":     filtered.apply(lambda r: {
                                "themes":    r.get("Themes", ""),
                                "locations": r.get("Locations", ""),
                                "tone":      r.get("Tone", ""),
                                "source_url": r.get("DocumentIdentifier", ""),
                            }, axis=1),
        })
        self.validate_staging(staged)
        return staged

    def _fetch_index(self) -> list[str]:
        resp = requests.get(GDELT_GKG_INDEX, timeout=30)
        resp.raise_for_status()
        return [line.split()[-1] for line in resp.text.strip().split("\n")]

    def _url_in_range(self, url: str, start: date, end: date) -> bool:
        # URL format: .../20240115120000.gkg.csv.zip
        try:
            fname = url.split("/")[-1]
            d = date(int(fname[:4]), int(fname[4:6]), int(fname[6:8]))
            return start <= d <= end
        except Exception:
            return False

    def _download_gkg_file(self, url: str) -> pd.DataFrame:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                return pd.read_csv(f, sep="\t", on_bad_lines="skip",
                                   low_memory=False, header=0,
                                   names=self._gkg_columns())

    def _gkg_columns(self) -> list[str]:
        return [
            "GKGRECORDID",           # col 0
            "DATE",                  # col 1
            "SourceCollectionID",    # col 2
            "DocumentIdentifier",    # col 3
            "SourceURL",             # col 4
            "Counts",                # col 5
            "V2Counts",              # col 6
            "Themes",                # col 7
            "V2Themes",              # col 8
            "Locations",             # col 9
            "V2Locations",           # col 10
            "Persons",               # col 11
            "V2Persons",             # col 12
            "Organizations",         # col 13
            "V2Organizations",       # col 14
            "Tone",                  # col 15
            "Dates",                 # col 16
            "GCAM",                  # col 17
            "SharingImage",          # col 18
            "RelatedImages",         # col 19
            "SocialImageEmbeds",     # col 20
            "SocialVideoEmbeds",     # col 21
            "Quotations",            # col 22
            "AllNames",              # col 23
            "Amounts",               # col 24
            "TranslationInfo",       # col 25
            "Extras",                # col 26
        ]