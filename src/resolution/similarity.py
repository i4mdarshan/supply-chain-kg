from __future__ import annotations
import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from sentence_transformers import SentenceTransformer
from loguru import logger
from functools import lru_cache

_SEMANTIC_MODEL = None

def get_semantic_model() -> SentenceTransformer:
    global _SEMANTIC_MODEL
    if _SEMANTIC_MODEL is None:
        logger.info("Loading sentence-transformer model...")
        _SEMANTIC_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Sentence-transformer loaded")
    return _SEMANTIC_MODEL


# String similarity
# RapidFuzz token_sort_ratio — handles word reordering. Eg: 'Apple Inc' vs 'Inc Apple'

def string_similarity(a: str, b: str) -> float:

    if not a or not b:
        return 0.0
    return fuzz.token_sort_ratio(
        a.lower().strip(),
        b.lower().strip()
    ) / 100.0


# Semantic similarity. Eg: 'Goldman Sachs' vs 'GS Bank' type matches
def semantic_similarity(a: str, b: str) -> float:

    if not a or not b:
        return 0.0
    model  = get_semantic_model()
    embeds = model.encode([a, b], normalize_embeddings=True)
    return float(np.dot(embeds[0], embeds[1]))


def batch_semantic_similarity(pairs: list[tuple[str, str]]) -> list[float]:
    if not pairs:
        return []

    model = get_semantic_model()
    unique = list({t for pair in pairs for t in pair})
    embeddings = model.encode(unique, normalize_embeddings=True, batch_size=128)
    embed_map  = {text: emb for text, emb in zip(unique, embeddings)}

    scores = []
    for a, b in pairs:
        if a not in embed_map or b not in embed_map:
            scores.append(0.0)
        else:
            scores.append(float(np.dot(embed_map[a], embed_map[b])))
    return scores


def spatial_similarity(
    a: str,
    b: str,
    co_occurrence_index: dict[str, set[str]],
) -> float:
    if not a or not b:
        return 0.0

    events_a = co_occurrence_index.get(a.lower(), set())
    events_b = co_occurrence_index.get(b.lower(), set())

    if not events_a or not events_b:
        return 0.0

    shared = len(events_a & events_b)
    denom  = max(len(events_a), len(events_b))
    return shared / denom if denom > 0 else 0.0


def build_co_occurrence_index(df: pd.DataFrame) -> dict[str, set[str]]:
    index  = {}
    gdelt  = df[df["source"] == "gdelt"]

    for _, row in gdelt.iterrows():
        key = str(row["entity_text"]).lower().strip()
        rid = str(row["raw_id"])
        if key not in index:
            index[key] = set()
        index[key].add(rid)

    logger.info(f"Built co-occurrence index: {len(index)} unique entities")
    return index


# Combined score

WEIGHTS = {
    "string":   0.35,
    "semantic": 0.40,
    "spatial":  0.25,
}

def combined_score(
    a: str,
    b: str,
    co_occurrence_index: dict[str, set[str]],
    string_w:   float = WEIGHTS["string"],
    semantic_w: float = WEIGHTS["semantic"],
    spatial_w:  float = WEIGHTS["spatial"],
) -> dict:
    s_str  = string_similarity(a, b)
    s_sem  = semantic_similarity(a, b)
    s_spa  = spatial_similarity(a, b, co_occurrence_index)

    score  = (string_w * s_str) + (semantic_w * s_sem) + (spatial_w * s_spa)

    return {
        "entity_a":       a,
        "entity_b":       b,
        "string_score":   round(s_str, 4),
        "semantic_score": round(s_sem, 4),
        "spatial_score":  round(s_spa, 4),
        "combined_score": round(score, 4),
    }