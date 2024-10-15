# Import required libraries
import pandas as pd # Data manipulation
from bs4 import BeautifulSoup # Extract text from XML
from time import time # Track time taken
from glob import glob # Gather files
import os # Folder and file creation
from sentence_transformers import SentenceTransformer # For embedding the text
import torch # For gpu 
from tqdm import tqdm # Progress bar

################################################################################

# Track time
start = time()

# Input folder and output file name
data_folder = 'biorxiv-xml-dump/'
output_file = 'biorxiv-embeddings.parquet'

# Gather XML files
print(f"Gathering XML files from {data_folder}")
xml_files = glob(data_folder + '*.xml')

# Make the app device agnostic
device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")

# Load a pretrained Sentence Transformer model and move it to the appropriate device
print(f"Loading model to device: {device}")
model = SentenceTransformer("mixedbread-ai/mxbai-embed-large-v1")
model = model.to(device)

################################################################################

def extract_abstract_doi(xml_file):
    
    with open(xml_file, 'r') as f:
        data = f.read()

    # Parse the XML data
    soup = BeautifulSoup(data, 'xml')

    # Get abstract
    abstract = soup.find('abstract').get_text(separator=" ",strip=True)

    # Get doi
    doi = soup.find('article-id', {'pub-id-type': 'doi'}).get_text()

    # Return dictionary
    return {'doi': doi, 'abstract': abstract}


# Function that does the embedding
def embed(input_text):
    
    # Calculate embeddings by calling model.encode(), specifying the device
    embedding = model.encode(input_text, device=device)

    return embedding

################################################################################

# Create an empty DataFrame to store the results
df = pd.DataFrame(columns=['id', 'vector'])

# Loop through each XML file
print(f"Looping through {len(xml_files)} XML files")
for xml_file in tqdm(xml_files):

    try:
        # Extract abstract and doi
        abstract_doi = extract_abstract_doi(xml_file)

        # Create embedding
        embedding = embed(abstract_doi['abstract'])

        # Create a dataframe row
        row = pd.DataFrame({'id': abstract_doi['doi'], 'vector': [embedding]})

        # Append the row to the DataFrame
        df = pd.concat([df, row], ignore_index=True)

    except Exception as e:
        with open('bioarxiv_errors.txt', 'a') as f:
            f.write(f"Error processing file {xml_file}: {e}\n")

# Save the DataFrame to a Paquet file
print(f"Saving DataFrame to parquet file: {output_file}")
df.to_parquet(output_file)

# Print time taken
print(f"Time taken: {time() - start} seconds")