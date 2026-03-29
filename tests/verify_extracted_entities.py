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


"""