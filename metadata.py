# File description:
# 1. This Python script is designed to extract metadata from a collection of XML files, which are assumed to be scientific articles from the medRxiv preprint server.
# 2. The script uses the BeautifulSoup library to parse the XML data and extract the article's abstract, DOI, title, and authors.
# 3. The extracted metadata is then stored in a pandas DataFrame, which is saved as a Parquet file for efficient storage and retrieval.
# 4. The script also includes a progress bar using the tqdm library to track the progress of the metadata extraction process.
# 5. The script is set up to handle a large number of XML files, making it suitable for processing large datasets.
# 6. The script uses caching to improve performance by avoiding redundant computations.

# Import required libraries
import pandas as pd # Data manipulation
from bs4 import BeautifulSoup # Extract text from XML
from time import time # Track time
from glob import glob # Gather XML files
import multiprocessing as mp # Parallel processing
################################################################################

# Track time
start = time()

# Input folder and output file name
prefix = 'bio'

data_folder = f'{prefix}rxiv-xml-dump/'
output_file = f'{prefix}rxiv_metadata.parquet'

# Gather XML files
print(f"Gathering XML files from {data_folder}")
xml_files = glob(data_folder + '*.xml')
################################################################################
def extract_info(xml_file):

    try:
    
        # Open and read the XML file
        with open(xml_file, 'r') as f:
            data = f.read()

        # Parse the XML data
        soup = BeautifulSoup(data, 'xml')

        # Get abstract
        abstract = soup.find('abstract').get_text(separator=" ",strip=True)

        # Get doi
        doi = soup.find('article-id', {'pub-id-type': 'doi'}).get_text()

        # Get article title
        article_title = soup.find('article-title').get_text(separator=" ",strip=True)

        # Get author names
        authors = soup.find_all('contrib', {'contrib-type':"author"})
        author_names = [res.get_text(separator=" ", strip=True) 
                for author in authors 
                if (res := author.find('name')) is not None]
        
        # Return dictionary
        return {
            "id": doi,
            "Title": article_title,
            "Authors": ', '.join(author_names), # Convert list of authors to one string
            "Abstract": abstract,
            "URL": f"https://doi.org/{doi}" # Construct URL from DOI
        }
    
    except Exception as e:
        with open(f'{prefix}rxiv_metadata_errors.txt', 'a') as f:
            f.write(f"Error processing file {xml_file}: {e}\n")
        return None

################################################################################

# Extract information from XML files
print(f"Extracting information from {len(xml_files)} XML files")

if __name__ == '__main__':
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(extract_info, xml_files)

    # Remove None values
    results = [result for result in results if result is not None]

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(results)

    # Save DataFrame to Parquet file
    df.to_parquet(output_file, index=False)

    # Print time taken
    print(f"Time taken: {time() - start} seconds")


