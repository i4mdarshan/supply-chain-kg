import pandas as pd
df = pd.read_parquet('data/processed/entities_extracted.parquet')

print('=== SHAPE ===')
print(f'Rows: {len(df):,}')

print('\n=== SAMPLE PORT ENTITIES ===')
ports = df[df['entity_label'] == 'PORT'][['entity_text', 'source', 'date']]
print(ports.head(5).to_string())

print('\n=== SAMPLE COUNTRY ENTITIES ===')
countries = df[df['entity_label'] == 'COUNTRY'][['entity_text', 'source', 'date']]
print(countries.head(5).to_string())

print('\n=== SAMPLE COMPANY ENTITIES — GDELT ===')
companies = df[(df['entity_label'] == 'COMPANY') & (df['source'] == 'gdelt')][['entity_text', 'date']]
print(companies.head(5).to_string())


"""
Expected Output:
Verify the entities if they are properly tagged by SpaCy 


Eg:
=== SHAPE ===
Rows: 32,708

=== SAMPLE PORT ENTITIES ===
        entity_text source        date
25   atlantic ocean  gdelt  2026-03-20
407  atlantic ocean  gdelt  2026-03-20
497  atlantic ocean  gdelt  2026-03-20
604      bering sea  gdelt  2026-03-20
869  atlantic ocean  gdelt  2026-03-20

=== SAMPLE COUNTRY ENTITIES ===
      entity_text source        date
32  united states  gdelt  2026-03-20
50  united states  gdelt  2026-03-20
51       linkedin  gdelt  2026-03-20
60  united states  gdelt  2026-03-20
84  united states  gdelt  2026-03-20

=== SAMPLE COMPANY ENTITIES — GDELT ===
                entity_text        date
4    oxford pharmaceuticals  2026-03-20
9               white house  2026-03-20
10   washington news bureau  2026-03-20
16  el ciudadano university  2026-03-20
17       education ministry  2026-03-20


"""