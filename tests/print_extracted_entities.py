import pandas as pd

df = pd.read_parquet('data/processed/entities_extracted.parquet')

# Show unique entity texts sample per label
for label in ['COMPANY', 'COUNTRY', 'PORT', 'PERSON']:
    sample = df[df['entity_label'] == label]['entity_text'].drop_duplicates().head(50).tolist()
    print(f'\n{label}: {sample}')

print('\nTotal unique entity texts:', df['entity_text'].nunique())
print('Total rows:', len(df))
