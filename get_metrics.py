# Code to get metrics apm.

import pandas as pd
import json

# Load the JSON data from the file
with open('/Users/longcaca/Downloads/example/ETL-Flow-DataCentric/logs/metrics-apm.json', 'r') as file:
    data = json.load(file)

# Extracting relevant information from the JSON structure
# Assuming 'hits' contains the relevant metrics
hits = data['hits']['hits']

# Create a list to store extracted records
extracted_data = []

for hit in hits:
    source = hit['_source']
    transaction = source.get('transaction', {})
    
    # Extracting relevant fields
    record = {
        # Timestamp
        'Timestamp': source.get('@timestamp', None),  # Adjust based on actual field names in your JSON
        # CPU
        'System CPU Usage': source.get('system.cpu.usage', None),  # Adjust based on actual field names in your JSON
        'Process CPU Usage': source.get('process.cpu.usage', None),  # Adjust based on actual field names in your JSON
        'System CPU Count': source.get('system.cpu.count', None),  # Adjust based on actual field names in your JSON
        
        'jvm_system_cpu_load_1m': source.get('process.runtime.jvm.system.cpu.load_1m', None),  # Adjust based on actual field names in your JSON
        'jvm_cpu_utilization': source.get('process.runtime.jvm.cpu.utilization', None),  # Adjust based on actual field names in your JSON
        'jvm_system_cpu_utilization': source.get('process.runtime.jvm.system.cpu.utilization', None),  # Adjust based on actual field names in your JSON
        
        # Memory
        
        'jvm.memory.committed': source.get('jvm.memory.committed', None),  # Adjust based on actual field names in your JSON
        'jvm.memory.max': source.get('jvm.memory.max', None),  # Adjust based on actual field names in your JSON
        'jvm.memory.used': source.get('jvm.memory.used', None),  # Adjust based on actual field names in your JSON
        
        'jvm.buffer.memory.used': source.get('jvm.buffer.memory.used', None),  
        'jvm.memory.usage.after.gc': source.get('jvm.memory.usage.after.gc', None),  # Adjust based on actual field names in your JSON
        'jvm.gc.memory.allocated': source.get('jvm.gc.memory.allocated', None),  # Adjust based on actual field names in your JSON
        'jvm.gc.memory.promoted': source.get('jvm.gc.memory.promoted', None),  # Adjust based on actual field names in your JSON
    
        'process.runtime.jvm.memory.init': source.get('process.runtime.jvm.memory.init', None),  # Adjust based on actual field names in your JSON
        'process.runtime.jvm.memory.limit': source.get('process.runtime.jvm.memory.limit', None),  # Adjust based on actual field names in your JSON
        'process.runtime.jvm.memory.usage': source.get('process.runtime.jvm.memory.usage', None),  # Adjust based on actual field names in your JSON
        'process.runtime.jvm.memory.committed': source.get('process.runtime.jvm.memory.committed', None),  # Adjust based on actual field names in your JSON
        'process.runtime.jvm.memory.usage_after_last_gc': source.get('process.runtime.jvm.memory.usage_after_last_gc', None),  # Adjust based on actual field names in your JSON
        
        'system.memory.utilization': source.get('system.memory.utilization', None),  # Adjust based on actual field names in your JSON
        'system.memory.usage': source.get('system.memory.usage', None),  # Adjust based on actual field names in your JSON
        
        'Latency': transaction.get('duration.histogram', {}).get('values', [None])[0],  # First value as latency
        'Error Rate': transaction.get('result', None),  # Adjust based on actual error rate representation
        'Number of Requests': source.get('_doc_count', 0)  # Total document count as number of requests
    }
    
    extracted_data.append(record)

# Create a DataFrame from the extracted data
df = pd.DataFrame(extracted_data)

# Display the DataFrame
print(df)
df.to_csv('/Users/longcaca/Downloads/example/ETL-Flow-DataCentric/metrics.csv'  ,  index = False)
