# Class TRACES
import pandas as pd
import json

# Load the JSON data from the file
with open('/Users/longcaca/Downloads/example/ETL-Flow-DataCentric/logs/traces-apm.json', 'r') as file:  # Replace with your actual file path
    data = json.load(file)

# Prepare a list to hold the extracted records
extracted_data = []

# Iterate through each hit in the JSON data
for hit in data['hits']['hits']:
    source = hit['_source']
    
    # Extract relevant fields
    transaction = source.get('transaction', {})
    
    # Ensure duration is accessed correctly and converted to seconds
    duration_us = transaction.get('duration', {}).get('us', 0)  # Default to 0 if not found
    
    record = { #
        'Timestamp': source.get('@timestamp', None),
        
        'transaction_name': transaction.get('name', ''),
        'transaction_duration': duration_us,
        'transaction_id': transaction.get('id', ''),
        'transaction_type': transaction.get('type', ''),
        
        'span_name': source.get('span', {}).get('name', ''),
        'span_duration': source.get('span', {}).get('duration', {}).get('us', 0),
        'span_subtype': source.get('span', {}).get('subtype', ''),
        'span_id': source.get('span', {}).get('id', ''),
        'span_id': source.get('span', {}).get('type', ''),
    }
    
    extracted_data.append(record)

# Create a DataFrame from the extracted data
df = pd.DataFrame(extracted_data)

# Print the first 10 rows of the DataFrame
print(df.head(10))
df.to_csv('/Users/longcaca/Downloads/example/ETL-Flow-DataCentric/traces.csv'  ,  index = False )

