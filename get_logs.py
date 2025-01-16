# Code to get logs. not yet into class. 


# LOGS 

import pandas as pd
import json

# Load the JSON data from a file
with open('/Users/longcaca/Downloads/example/ETL-Flow-DataCentric/logs/logs-apm.json', 'r') as file:  # Replace with your actual file path
    data = json.load(file)

# Extracting relevant fields from the nested structure
extracted_data = []

for hit in data['hits']['hits']:
    source = hit['_source']
    error_info = source.get('error', {})
    stacktrace = error_info.get('stacktrace', [])
    
    # Prepare a dictionary for each record
    record = { 
        'Timestamp': source.get('@timestamp', None),
        'Message': source.get('message', ''),
        'Error code': error_info.get('exception', [{}])[0].get('type', ''),  # Get first exception message as Error cause
        'Error cause': error_info.get('exception', [{}])[0].get('message', '')  # Get first exception message as Error cause
    }
    extracted_data.append(record)

# Create a DataFrame from the extracted data
df = pd.DataFrame(extracted_data)

# Print the first 10 rows of the DataFrame
print(df.head(10))
df.to_csv('/Users/longcaca/Downloads/example/ETL-Flow-DataCentric/logs.csv' , index= False)









