# src/extraction/entity_staging.py
from pathlib import Path
import pandas as pd
from loguru import logger
from .ner_pipeline import NERPipeline
from .sanctions_cleaner import SanctionsCleaner

DATA_DIR = Path("data")

def run_extraction() -> pd.DataFrame:
    """
    Full T2 pipeline:
    1. Load entities_staging.parquet from T1
    2. Clean sanctions surface forms
    3. Run NER on GDELT + NewsAPI rows
    4. Merge Comtrade staging if available
    5. Explode extracted entities into one row per entity
    6. Save to data/processed/entities_extracted.parquet
    """

    # Load T1 output
    t1_path = DATA_DIR / "processed/entities_staging.parquet"
    logger.info(f"Loading T1 staging: {t1_path}")
    df = pd.read_parquet(t1_path)
    logger.info(f"Loaded {len(df)} records")

    # Step 1 — Clean sanctions surface forms
    cleaner = SanctionsCleaner()
    df = cleaner.clean(df)

    # Step 2 — Run NER on GDELT + NewsAPI
    ner = NERPipeline()
    df  = ner.run_on_staging(df)

    # Step 3 — Merge Comtrade if teammate has run their pipeline
    comtrade_path = DATA_DIR / "raw/comtrade/comtrade_staging.parquet"
    if comtrade_path.exists():
        logger.info("Comtrade staging found — merging")
        comtrade = pd.read_parquet(comtrade_path)

        # Comtrade rows don't need NER — wrap as single entity
        comtrade["extracted_entities"] = comtrade.apply(
            lambda r: [{
                "text":  r["surface_form"],
                "label": r["entity_type"],
                "start": 0,
                "end":   len(str(r["surface_form"])),
            }],
            axis=1
        )
        df = pd.concat([df, comtrade], ignore_index=True)
        logger.info(f"Total after Comtrade merge: {len(df)} records")
    else:
        logger.warning(
            "Comtrade staging not found — skipping. "
        )

    # Step 4 — Explode into one row per extracted entity
    logger.info("Exploding entities — one row per entity")
    exploded = df.explode("extracted_entities").reset_index(drop=True)
    exploded = exploded[exploded["extracted_entities"].notna()]

    # Flatten the entity dict into columns
    entity_df = pd.json_normalize(exploded["extracted_entities"])
    exploded  = exploded.drop(columns=["extracted_entities"]).reset_index(drop=True)
    result    = pd.concat([exploded, entity_df], axis=1)

    # Rename for clarity
    result = result.rename(columns={
        "text":          "entity_text",
        "label":         "entity_label",
        "surface_form":  "original_surface_form",
    })

    # Drop empty entity texts
    result = result[result["entity_text"].str.strip().str.len() > 1]

    out = DATA_DIR / "processed/entities_extracted.parquet"
    result.to_parquet(out)

    logger.info("=== T2 Complete ===")
    logger.info(f"Total extracted entities: {len(result)}")
    print("\nBreakdown by source:")
    print(
        result.groupby("source")[["raw_id"]]
        .count()
        .rename(columns={"raw_id": "entity_count"})
        .to_string()
    )
    print("\nBreakdown by entity label:")
    print(
        result.groupby("entity_label")[["raw_id"]]
        .count()
        .rename(columns={"raw_id": "entity_count"})
        .to_string()
    )
    logger.info(f"Saved -> {out}")
    return result