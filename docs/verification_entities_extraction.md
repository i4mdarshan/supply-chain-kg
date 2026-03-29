=== SHAPE ===
Rows: 27,925  |  Columns: 11

=== COLUMNS ===
['raw_id', 'original_surface_form', 'entity_type', 'source', 'date', 'confidence', 'metadata', 'entity_text', 'entity_label', 'start', 'end']

=== NULL CHECK ===
raw_id                   0
original_surface_form    0
entity_type              0
source                   0
date                     0
confidence               0
metadata                 0
entity_text              0
entity_label             0
start                    0
end                      0
dtype: int64

=== SAMPLE PORT ENTITIES ===
                                                      entity_text source        date
96                                                brooks district  gdelt  2026-03-20
246                                     agency;european union;s p  gdelt  2026-03-20
289  sankoo;union home;ladakh autonomous hill development council  gdelt  2026-03-20
451                                  mullin;pete hegseth;benjamin  gdelt  2026-03-20
637                                                          asia  gdelt  2026-03-20

=== SAMPLE COUNTRY ENTITIES ===
                   entity_text source        date
23                 party;pemex  gdelt  2026-03-20
25         trump united states  gdelt  2026-03-20
50         ministry;moratorium  gdelt  2026-03-20
56              services;royal  gdelt  2026-03-20
57  police;governmentofalberta  gdelt  2026-03-20

=== SAMPLE COMPANY ENTITIES — GDELT ===
                                                             entity_text        date
0                                           neely oxford pharmaceuticals  2026-03-20
2                                                           trump united  2026-03-20
3                         states;white house;washington news bureau;oval  2026-03-20
5  ciudadano university of san sebasti;education ministry;superintendent  2026-03-20
6                                                           hankey;ambra  2026-03-20




.venv/bin/python3 script_run_extraction.py
2026-03-29 11:15:31.134 | INFO     | src.extraction.entity_staging:run_extraction:23 - Loading T1 staging: data/processed/entities_staging.parquet
2026-03-29 11:15:31.259 | INFO     | src.extraction.entity_staging:run_extraction:25 - Loaded 25746 records
2026-03-29 11:15:31.290 | INFO     | src.extraction.sanctions_cleaner:clean:39 - Cleaned 24547 sanctions rows ({'ofac': 18707, 'eu_sanctions': 5840})
2026-03-29 11:15:31.290 | INFO     | src.extraction.ner_pipeline:__init__:22 - Loading spaCy model: en_core_web_lg
2026-03-29 11:15:31.905 | INFO     | src.extraction.ner_pipeline:__init__:24 - spaCy model loaded
2026-03-29 11:15:31.908 | INFO     | src.extraction.ner_pipeline:run_on_staging:54 - Running NER on 1199 rows (GDELT + NewsAPI)
2026-03-29 11:15:36.164 | INFO     | src.extraction.ner_pipeline:run_on_staging:82 - Passing through 24547 sanctions rows as-is
2026-03-29 11:15:36.237 | INFO     | src.extraction.ner_pipeline:run_on_staging:94 - NER complete
2026-03-29 11:15:36.238 | WARNING  | src.extraction.entity_staging:run_extraction:54 - Comtrade staging not found — skipping. 
2026-03-29 11:15:36.238 | INFO     | src.extraction.entity_staging:run_extraction:59 - Exploding entities — one row per entity
2026-03-29 11:15:36.372 | INFO     | src.extraction.entity_staging:run_extraction:81 - === T2 Complete ===
2026-03-29 11:15:36.372 | INFO     | src.extraction.entity_staging:run_extraction:82 - Total extracted entities: 32708

Breakdown by source:
              entity_count
source                    
eu_sanctions          5840
gdelt                 7994
newsapi                172
ofac                 18702

Breakdown by entity label:
                  entity_count
entity_label                  
COMPANY                  13888
COUNTRY                    747
DISRUPTION_EVENT            10
PERSON                   17938
PORT                       117
PRODUCT                      8
2026-03-29 11:15:36.376 | INFO     | src.extraction.entity_staging:run_extraction:97 - Saved -> data/processed/entities_extracted.parquet