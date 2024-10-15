# File description:
# This script merges two dataframes, one containing embeddings and the other containing metadata, on the 'id' column.
# It then creates a new column '$meta' which is a json string of the metadata.
# Finally, it drops the unnecessary columns and saves the merged dataframe to a parquet file.
# The output file is compatible with Milvus, a vector database.

# Import required libraries
import pandas as pd
import json

# Declare filenames
prefix = 'med'
embeddings_file = f'{prefix}rxiv_embeddings.parquet'
metadata_file = f'{prefix}rxiv_metadata.parquet'    
output_file = f'{prefix}rxiv_embeddings_metadata.parquet'

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

