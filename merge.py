import pandas as pd
import json

# Declare filenames
embeddings_file = 'medrxiv_embeddings.parquet'
metadata_file = 'medrxiv_metadata.parquet'    
output_file = 'medrxiv_embeddings_metadata.parquet'

# Load dataframes
embeddings = pd.read_parquet(embeddings_file)
metadata = pd.read_parquet(metadata_file)

# Merge dataframes on 'id'
merged = pd.merge(embeddings, metadata, on='id')

# Create milvus compatible parquet file
# It has 3 columns, id, vector, $meta, where
# id is the paper_id, vector is the embedding, and $meta is a json string of the metadata
merged['$meta'] = merged[['Title', 'Authors', 'Abstract', 'URL']].apply(lambda row: json.dumps(row.to_dict()), axis=1)

# Drop unnecessary columns
merged.drop(columns=['Title', 'Abstract', 'Authors', 'URL'], inplace=True)

# Save to parquet
merged.to_parquet(output_file, index=False)

