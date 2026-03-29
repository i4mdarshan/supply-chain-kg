import pandas as pd
from pathlib import Path

df = pd.read_parquet('data/processed/entities_staging.parquet')

print('=== SHAPE ===')
print(f'Rows: {df.shape[0]:,}  |  Columns: {df.shape[1]}')

print('\n=== COLUMNS ===')
print(df.columns.tolist())

print('\n=== BREAKDOWN BY SOURCE ===')
print(df.groupby('source')[['raw_id']].count().rename(columns={'raw_id': 'record_count'}).to_string())

print('\n=== BREAKDOWN BY ENTITY TYPE ===')
print(df.groupby('entity_type')[['raw_id']].count().rename(columns={'raw_id': 'record_count'}).to_string())

print('\n=== NULL CHECK ===')
print(df.isnull().sum())

print('\n=== CONFIDENCE DISTRIBUTION ===')
print(df['confidence'].value_counts().to_string())

print('\n=== SAMPLE — GDELT ===')
print(df[df['source']=='gdelt'][['raw_id','surface_form','date']].head(3).to_string())

print('\n=== SAMPLE — OFAC ===')
print(df[df['source']=='ofac'][['raw_id','surface_form','entity_type']].head(3).to_string())

print('\n=== SAMPLE — EU SANCTIONS ===')
print(df[df['source']=='eu_sanctions'][['raw_id','surface_form','entity_type']].head(3).to_string())

print('\n=== SAMPLE — NEWSAPI ===')
print(df[df['source']=='newsapi'][['raw_id','surface_form','date']].head(3).to_string())



"""
Expected Output:

Total rows should be ~25,000+
All 7 columns present with zero nulls on critical fields
All 4 sources represented in the breakdown
Surface forms contain real names/text, not empty strings or garbage values
Confidence values are 1.0 for sanctions sources, 0.6-0.7 for GDELT/NewsAPI


"""