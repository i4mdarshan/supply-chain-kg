# run_t1.py
from pathlib import Path
from datetime import date
from loguru import logger

from src.acquisition.gdelt import GDELTAcquirer
from src.acquisition.ofac import OFACAcquirer
from src.acquisition.eu_sanctions import EUSanctionsAcquirer
from src.acquisition.newsapi import NewsAPIAcquirer

import pandas as pd

DATA_DIR = Path("data")

def get_yes_no_input(prompt: str) -> bool:
    """Get yes/no input from user."""
    while True:
        response = input(f"{prompt} (yes/no): ").strip().lower()
        if response in ('yes', 'y'):
            return True
        elif response in ('no', 'n'):
            return False
        else:
            print("Please enter 'yes' or 'no'")

def run_all():
    logger.info("=== T1: Data Acquisition ===")
    staging_frames = []

    # User Input 1: GDELT
    if get_yes_no_input("\n[1/5] Run GDELT?"):
        logger.info("--- GDELT ---")
        try:
            gdelt = GDELTAcquirer(DATA_DIR / "raw/gdelt")
            raw = gdelt.fetch(
                start=date(2026, 3, 20),
                end=date(2026, 3, 28),
                max_files=60
            )
            staging_frames.append(gdelt.to_staging(raw))
            logger.info("GDELT completed")
        except Exception as e:
            logger.error(f"Failed to fetch GDELT: {e}")
    else:
        logger.info("GDELT skipped")

    # User Input 2: OFAC
    if get_yes_no_input("\n[2/5] Run OFAC?"):
        logger.info("--- OFAC ---")
        try:
            ofac = OFACAcquirer(DATA_DIR / "raw/ofac")
            raw = ofac.fetch()
            staging_frames.append(ofac.to_staging(raw))
            logger.info("OFAC completed")
        except Exception as e:
            logger.error(f"Failed to fetch OFAC: {e}")
    else:
        logger.info("OFAC skipped")

    # User Input 3: EU Sanctions
    if get_yes_no_input("\n[3/5] Run EU Sanctions?"):
        logger.info("--- EU Sanctions ---")
        try:
            eu = EUSanctionsAcquirer(DATA_DIR / "raw/eu_sanctions")
            raw_eu = eu.fetch()
            df_eu = eu.to_staging(raw_eu)
            staging_frames.append(df_eu)
            logger.info("EU Sanctions completed")
        except Exception as e:
            logger.error(f"Failed to fetch EU Sanctions: {e}")
    else:
        logger.info("EU Sanctions skipped")

    # User Input 4: NewsAPI
    if get_yes_no_input("\n[4/5] Run NewsAPI?"):
        logger.info("--- NewsAPI ---")
        try:
            news = NewsAPIAcquirer(DATA_DIR / "raw/newsapi")
            raw_news = news.fetch(days_back=29)
            df_news = news.to_staging(raw_news)
            staging_frames.append(df_news)
            logger.info("NewsAPI completed")
        except EnvironmentError as e:
            logger.warning(f"Failed to fetch NewsAPI: {e}")
        except Exception as e:
            logger.error(f"Failed to fetch NewsAPI: {e}")
    else:
        logger.info("NewsAPI skipped")

    # User Input 5: Combine staging files
    if get_yes_no_input("\n[5/5] Combine staging files into final dataset?"):
        combine_staging_files()
    else:
        logger.info("Combining staging files skipped")

def combine_staging_files():
    frames = []

    # GDELT
    gdelt_staging = list((DATA_DIR / 'raw/gdelt').glob('*staging*.parquet'))
    if gdelt_staging:
        df = pd.read_parquet(gdelt_staging[0])
        frames.append(df)
        logger.info(f"GDELT: {len(df)} records")

    # OFAC
    ofac_staging = DATA_DIR / 'raw/ofac/ofac_staging.parquet'
    if ofac_staging.exists():
        df = pd.read_parquet(ofac_staging)
        frames.append(df)
        logger.info(f"OFAC: {len(df)} records")

    # EU Sanctions
    eu_staging = DATA_DIR / 'raw/eu_sanctions/eu_sanctions_staging.parquet'
    if eu_staging.exists():
        df = pd.read_parquet(eu_staging)
        frames.append(df)
        logger.info(f"EU Sanctions: {len(df)} records")

    # NewsAPI
    news_staging = DATA_DIR / 'raw/newsapi/newsapi_staging.parquet'
    if news_staging.exists():
        df = pd.read_parquet(news_staging)
        frames.append(df)
        logger.info(f"NewsAPI: {len(df)} records")

    if not frames:
        logger.error("No staging files found — run acquisition steps first")
        return

    # Merge
    combined = pd.concat(frames, ignore_index=True)
    out = DATA_DIR / "processed/entities_staging.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out)

    logger.info("=== T1 Complete ===")
    logger.info(f"Total staged records: {len(combined)}")
    print("\nBreakdown by source:")
    print(
        combined.groupby("source")[["raw_id"]]
        .count()
        .rename(columns={"raw_id": "record_count"})
        .to_string()
    )
    logger.info(f"Unified staging table -> {out}")


if __name__ == "__main__":
    run_all()