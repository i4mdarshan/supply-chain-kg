.venv/bin/python3 script_run_extraction.py
2026-03-29 10:52:43.565 | INFO     | src.extraction.entity_staging:run_extraction:23 - Loading T1 staging: data/processed/entities_staging.parquet
2026-03-29 10:52:43.707 | INFO     | src.extraction.entity_staging:run_extraction:25 - Loaded 25746 records
2026-03-29 10:52:43.740 | INFO     | src.extraction.sanctions_cleaner:clean:39 - Cleaned 24547 sanctions rows ({'ofac': 18707, 'eu_sanctions': 5840})
2026-03-29 10:52:43.740 | INFO     | src.extraction.ner_pipeline:__init__:22 - Loading spaCy model: en_core_web_lg
2026-03-29 10:52:44.329 | INFO     | src.extraction.ner_pipeline:__init__:24 - spaCy model loaded
2026-03-29 10:52:44.332 | INFO     | src.extraction.ner_pipeline:run_on_staging:59 - Running NER on 1199 rows (GDELT + NewsAPI)
2026-03-29 10:52:46.916 | INFO     | src.extraction.ner_pipeline:run_on_staging:66 - Passing through 24547 sanctions rows as-is
2026-03-29 10:52:46.991 | INFO     | src.extraction.ner_pipeline:run_on_staging:78 - NER complete
2026-03-29 10:52:46.992 | WARNING  | src.extraction.entity_staging:run_extraction:54 - Comtrade staging not found — skipping. Teammate should run their acquisition + comtrade_parser.py
2026-03-29 10:52:46.992 | INFO     | src.extraction.entity_staging:run_extraction:60 - Exploding entities — one row per entity
2026-03-29 10:52:47.111 | INFO     | src.extraction.entity_staging:run_extraction:82 - === T2 Complete ===
2026-03-29 10:52:47.111 | INFO     | src.extraction.entity_staging:run_extraction:83 - Total extracted entities: 27925

Breakdown by source:
              entity_count
source                    
eu_sanctions          5840
gdelt                 3201
newsapi                182
ofac                 18702

Breakdown by entity label:
                  entity_count
entity_label                  
COMPANY                  12611
COUNTRY                    372
DISRUPTION_EVENT            13
PERSON                   14835
PORT                        55
PRODUCT                     39
2026-03-29 10:52:47.115 | INFO     | src.extraction.entity_staging:run_extraction:98 - Saved -> data/processed/entities_extracted.parquet