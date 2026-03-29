import spacy
import pandas as pd
from pathlib import Path
from loguru import logger

# Entity label mapping — spaCy labels -> our ontology types
LABEL_MAP = {
    "ORG":     "COMPANY",
    "GPE":     "COUNTRY",
    "LOC":     "PORT",
    "PERSON":  "PERSON",
    "PRODUCT": "PRODUCT",
    "EVENT":   "DISRUPTION_EVENT",
}

# Only extract these spaCy labels
RELEVANT_LABELS = set(LABEL_MAP.keys())

class NERPipeline:

    def __init__(self, model: str = "en_core_web_lg"):
        logger.info(f"Loading spaCy model: {model}")
        self.nlp = spacy.load(model)
        logger.info("spaCy model loaded")

    def extract(self, texts: list[str]) -> list[list[dict]]:
        """
        Run NER over a list of texts.
        Returns a list of entity lists — one per input text.
        Each entity is a dict: {text, label, start, end}
        """
        results = []
        # pipe() is faster than calling nlp() in a loop
        for doc in self.nlp.pipe(texts, batch_size=64, disable=["parser"]):
            entities = [
                {
                    "text":  ent.text.strip(),
                    "label": LABEL_MAP.get(ent.label_, ent.label_),
                    "start": ent.start_char,
                    "end":   ent.end_char,
                }
                for ent in doc.ents
                if ent.label_ in RELEVANT_LABELS
                and len(ent.text.strip()) > 1
            ]
            results.append(entities)
        return results

    def run_on_staging(self, staging_df: pd.DataFrame) -> pd.DataFrame:
        needs_ner = staging_df["source"].isin(["gdelt", "newsapi"])
        ner_rows  = staging_df[needs_ner].copy()
        skip_rows = staging_df[~needs_ner].copy()

        logger.info(f"Running NER on {len(ner_rows)} rows (GDELT + NewsAPI)")

        # GDELT surface_form is semicolon-separated — split and clean each part
        # NewsAPI surface_form is full sentence — pass as-is
        def prepare_text(row):
            if row["source"] == "gdelt":
                # Split on semicolon, clean each part, rejoin as sentences
                parts = [p.strip() for p in str(row["surface_form"]).split(";")]
                parts = [p for p in parts if len(p) > 2]
                return ". ".join(parts)
            return str(row["surface_form"])

        texts    = ner_rows.apply(prepare_text, axis=1).tolist()
        entities = self.extract(texts)
        ner_rows["extracted_entities"] = entities

        # Filter out low quality extractions — entities that still contain semicolons
        # or are just single words under 3 chars
        def filter_entities(ent_list):
            return [
                e for e in ent_list
                if ";" not in e["text"]
                and len(e["text"].strip()) >= 3
                and not e["text"].strip().isdigit()
            ]

        ner_rows["extracted_entities"] = ner_rows["extracted_entities"].apply(filter_entities)

        logger.info(f"Passing through {len(skip_rows)} sanctions rows as-is")
        skip_rows["extracted_entities"] = skip_rows.apply(
            lambda r: [{
                "text":  r["surface_form"],
                "label": r["entity_type"],
                "start": 0,
                "end":   len(r["surface_form"]),
            }],
            axis=1
        )

        combined = pd.concat([ner_rows, skip_rows], ignore_index=True)
        logger.info("NER complete")
        return combined