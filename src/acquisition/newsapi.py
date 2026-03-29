# src/acquisition/newsapi.py
from pathlib import Path
from datetime import date, timedelta
import requests
import pandas as pd
import json
import time
import os
from loguru import logger
from dotenv import load_dotenv
from .base import BaseAcquirer

load_dotenv()

NEWSAPI_BASE = "https://newsapi.org/v2/everything"

# Targeted supply chain queries — kept small to respect 100 req/day free limit
QUERIES = [
    "supply chain disruption",
    "port shutdown shipping delay",
    "semiconductor shortage trade",
    "export ban trade sanction",
    "factory closure industrial shutdown",
]

class NewsAPIAcquirer(BaseAcquirer):

    def fetch(self, days_back: int = 29, **kwargs) -> Path:
        api_key = os.getenv("NEWSAPI_KEY")
        if not api_key or api_key == "your_key_here":
            raise EnvironmentError(
                "NEWSAPI_KEY not set in .env — "
                "register free at https://newsapi.org/register"
            )

        end_date   = date.today()
        start_date = end_date - timedelta(days=days_back)
        # Free tier only allows articles from last 30 days
        logger.info(f"NewsAPI fetch: {start_date} -> {end_date}")
        logger.info(f"Running {len(QUERIES)} queries (free tier: 100 req/day)")

        all_articles = []
        for i, query in enumerate(QUERIES):
            logger.info(f"  Query {i+1}/{len(QUERIES)}: '{query}'")
            try:
                resp = requests.get(
                    NEWSAPI_BASE,
                    params={
                        "q":        query,
                        "language": "en",
                        "sortBy":   "relevancy",
                        "pageSize": 20,
                        "apiKey":   api_key,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data     = resp.json()
                articles = data.get("articles", [])

                # Tag each article with the query that produced it
                for a in articles:
                    a["_query"] = query
                all_articles.extend(articles)
                logger.info(f"  Got {len(articles)} articles")

                # Be polite — avoid hitting rate limits
                time.sleep(1)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 426:
                    logger.warning(f"  426 on query '{query}' — retrying once after 2s")
                    time.sleep(2)
                    try:
                        resp = requests.get(
                            NEWSAPI_BASE,
                            params={
                                "q":        query,
                                "language": "en",
                                "sortBy":   "relevancy",
                                "pageSize": 20,
                                "apiKey":   api_key,
                            },
                            timeout=30,
                        )
                        resp.raise_for_status()
                        articles = resp.json().get("articles", [])
                        for a in articles:
                            a["_query"] = query
                        all_articles.extend(articles)
                        logger.info(f"  Retry succeeded — got {len(articles)} articles")
                    except Exception as retry_err:
                        logger.warning(f"  Retry failed: {retry_err}")
                else:
                    logger.warning(f"  Query failed: {e}")

        # Deduplicate by URL
        seen     = set()
        unique   = []
        for a in all_articles:
            url = a.get("url", "")
            if url and url not in seen:
                seen.add(url)
                unique.append(a)

        logger.info(f"Total articles after dedup: {len(unique)}")

        out = self.output_dir / "newsapi_raw.json"
        out.write_text(json.dumps(unique, indent=2))
        logger.info(f"Saved {len(unique)} articles -> {out}")
        return out

    def to_staging(self, raw_path: Path) -> pd.DataFrame:
        logger.info("Staging NewsAPI articles...")
        articles = json.loads(raw_path.read_text())

        records = []
        for a in articles:
            source_name = (a.get("source") or {}).get("name", "")
            title       = a.get("title") or ""
            description = a.get("description") or ""

            # Use title + description as surface form for NER in T2
            surface = f"{title}. {description}".strip(". ")
            if not surface:
                continue

            records.append({
                "raw_id":       f"newsapi_{abs(hash(a.get('url', '')))}",
                "surface_form": surface,
                "entity_type":  "DISRUPTION_EVENT",
                "source":       "newsapi",
                "date":         (a.get("publishedAt") or "")[:10],
                "confidence":   0.6,
                "metadata": {
                    "source_name": source_name,
                    "url":         a.get("url", ""),
                    "query":       a.get("_query", ""),
                    "title":       title,
                },
            })

        df = pd.DataFrame(records)
        self.validate_staging(df)

        out = self.output_dir / "newsapi_staging.parquet"
        df.to_parquet(out)
        logger.info(f"Staged {len(df)} NewsAPI articles -> {out}")
        return df