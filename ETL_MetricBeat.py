from datetime import datetime, timedelta
from urllib import request
import urllib.request
import requests
from tqdm import tqdm
import warnings
from typing import Dict, List, Any
import os

class ExtractMetricBeatLogs:
    def __init__(
        self, 
        url:str="https://116.101.122.180:5200/metricbeat-*/_search",
        api_key:str=None,
        start_time:str="2024-12-14T00:00:00.000Z", 
        end_time:str="2024-12-15T00:00:00.000Z", 
        step:int=500,
        limit:int=5000
    ):
        warnings.filterwarnings("ignore")

        self.url = url
        self.__api_key = api_key
        self.start_time = start_time
        self.end_time = end_time
        self.step = step
        self.limit = limit
    
    @property    
    def __headers(self):
        return {
            "Authorization": f"ApiKey {self.__api_key}",
            "Content-Type": "application/json"
        }
        
    def __data(self, query_time):
        return {
            "from": 0,
            "size": self.limit,
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": "cee25daa-3fd9-441b-af33-8211e3649f3e",
                                "default_operator": "AND"
                            }
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": self.start_time,
                                    "lte": query_time,
                                    "format": "strict_date_optional_time"
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "asc"
                    }
                }
            ]
        }
    
    def process_response(self, response):
        if response.status_code!=200:
            return False
        else:
            data = response.json()
            length = data['hits']["total"]["value"]
            miss_count = self.limit-length
            logs = data['hits']["hits"]
            return logs, length, miss_count
        
    def get_log(self)->List[Dict[str, Any]]:
        LOGS = []
        
        start_dt = datetime.strptime(self.start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        end_dt = datetime.strptime(self.end_time, "%Y-%m-%dT%H:%M:%S.%fZ") 
        delta = end_dt - start_dt
        milliseconds = int(delta.total_seconds() * 1000)
        
        for _ in tqdm(range(0, milliseconds, self.step), desc="Extracting"):
            current_time = datetime.strptime(self.start_time, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(milliseconds=self.step)
            query_time = current_time.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
            try:
                response = requests.get(self.url, headers=self.__headers, json=self.__data(query_time), verify=False, timeout=15)
                if self.process_response(response):
                    log, length, miss_count = self.process_response(response)
                    if miss_count<0:
                        print(f"Missed {abs(miss_count)} lines from metricbeat-Logging")
                    LOGS+=log
            except Exception as e:
                print(f"An error occurred: {type(e).__name__} - {e}")
            
            self.start_time = current_time.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
        return LOGS
   
class Transform():
    def __init__(self, logs:List=None):
        self.logs = logs
        
    def extract_system_resource_logs(self, entry):
        # data stream type. 
        log_source = entry.get("_source", {})
        timestamp = log_source.get("@timestamp", "N/A")
        host_name = log_source.get("host", {}).get("name", "N/A") # or host os name
        metricset = log_source.get("data_stream.type", {})
        
        # Extract Metrics 
        if metricset == "metrics":
            transaction = log_source.get('transaction', {})
            
            system_cpu_usage = log_source.get('system.cpu.usage', None)
            process_cpu_usage = log_source.get('process.cpu.usage ', None)
            system_cpu_count = log_source.get('system.cpu.count', None)
            ## change here *** 
            jvm_info = log_source.get('process.runtime.jvm', {})
            jvm_system_cpu_load_1m = jvm_info.get('system.cpu.load_1m', None)
            jvm_cpu_utilization = jvm_info.get('cpu.utilization', None)
            jvm_system_cpu_utilization = jvm_info.get('system.cpu.utilization', None)

            # Extracting memory-related information
            jvm_memory = log_source.get('jvm.memory', {})
            jvm_memory_committed = jvm_memory.get('committed', None)
            jvm_memory_max = jvm_memory.get('max', None)
            jvm_memory_used = jvm_memory.get('used', None)

            jvm_buffer_memory_used = log_source.get('jvm.buffer.memory.used', None)
            jvm_memory_usage_after_gc = log_source.get('jvm.memory.usage.after.gc', None)
            jvm_gc_memory_allocated = log_source.get('jvm.gc.memory.allocated', None)
            jvm_gc_memory_promoted = log_source.get('jvm.gc.memory.promoted', None)

            process_runtime_jvm_memory = jvm_info.get('memory', {})
            process_runtime_jvm_memory_init = process_runtime_jvm_memory.get('init', None)
            process_runtime_jvm_memory_limit = process_runtime_jvm_memory.get('limit', None)
            process_runtime_jvm_memory_usage = process_runtime_jvm_memory.get('usage', None)
            process_runtime_jvm_memory_committed = process_runtime_jvm_memory.get('committed', None)
            process_runtime_jvm_memory_usage_after_last_gc = process_runtime_jvm_memory.get('usage_after_last_gc', None)

            # Extracting system memory utilization
            system_memory_utilization = log_source.get('system.memory.utilization', None)
            system_memory_usage = log_source.get('system.memory.usage', None)

            # Extracting transaction-related information
            latency = transaction.get('duration.histogram', {}).get('values', [None])[0]
            error_rate = transaction.get('result', None)  # Adjust based on actual error rate representation
            number_of_requests = log_source.get('_doc_count', 0)  # Total document count as number of requests
            
            logline = (
                f"[{timestamp}] | HOST: {host_name} | "
                f"message: {str(message)}, "
                f"error_info: {str(error_info)}, "
                f"error_code: {str(error_code)}, "
                f"error_cause: {str(error_cause)}"
            )
            return logline



        
        # Extract logs 
        elif metricset == "logs":
            timestamp = log_source.get('@timestamp', None)
            message = log_source.get('message', '')
            # Extracting error information
            error_info = log_source.get('error', {})  # Assuming error_info is part of source
            error_code = error_info.get('exception', [{}])[0].get('type', '')  # Get first exception type
            error_cause = error_info.get('exception', [{}])[0].get('message', '')  # Get first exception message
            
            logline = (
                f"[{timestamp}] | HOST: {host_name} "
                f"message: {str(message)}" 
                f"error_info: {str(error_info)}" 
                f"error_code: {str(error_code)}"
                f"error_cause: {str(error_cause)}"
            )
            return logline
        
        # Extract system load
        elif metricset == "traces":
            # Assuming 'source' and 'transaction' are already defined as dictionaries from your JSON data

# Extracting transaction information
            transaction = log_source.get('transaction', {})

            # Ensure duration is accessed correctly and converted to seconds
            duration_us = transaction.get('duration', {}).get('us', 0)  # Default to 0 if not found

            # Extracting relevant information into variables
            timestamp = log_source.get('@timestamp', None)
            transaction_name = transaction.get('name', '')
            transaction_duration = duration_us / 1_000_000  # Convert microseconds to seconds
            transaction_id = transaction.get('id', '')
            transaction_type = transaction.get('type', '')

            # Extracting span information
            span = log_source.get('span', {})
            span_name = span.get('name', '')
            span_duration = span.get('duration', {}).get('us', 0)  # Duration in microseconds
            span_subtype = span.get('subtype', '')
            span_id = span.get('id', '')
            span_type = span.get('type', '')
            
            
            logline = (
                f"[{timestamp}] | HOST: {host_name} "
                f"transaction_name: {str(transaction_name)}" 
                f"transaction_duration: {str(transaction_duration)}" 
                f"transaction_id: {str(transaction_id)}"
                f"transaction_type: {str(transaction_type)}" 
            
                f"span_name: {str(span_name)}" 
                f"span_duration: {str(span_duration)}" 
                f"span_subtype: {str(span_subtype)}"
                f"span_type: {str(span_type)}" 
                f"span_id: {str(span_id)}" 
            )
            return logline
        
        # Extract Networks
        # elif metricset == "network":
        #     network_info = log_source.get("system", {}).get("network", {})
        #     interface_name = network_info.get("name", "N/A")
        #     in_bytes = network_info.get("in", {}).get("bytes", 0) / (1024**2) #BYTES
        #     out_bytes = network_info.get("out", {}).get("bytes", 0) / (1024**2)
        #     # logline: 
        #     logline = (
        #         f"[{timestamp}] ENV: {env} | HOST: {host_name} | NETWORK ({interface_name}): "
        #         f"IN: {in_bytes:.2f} MB, OUT: {out_bytes:.2f} MB"
        #     )
        #     return logline
        else:
            return None

    
    def exact_log(self):
        LOGS_EXTRACTED = [self.extract_system_resource_logs(log)+"\n" for log in tqdm(self.logs, desc="Transforming")]
        return LOGS_EXTRACTED         

class Load:
    def __init__(self, logs:List, log_info:Dict, save_dir:str):
        self.logs = logs
        self.log_info = log_info
        self.save_dir = save_dir
        self.run()
    
    @property
    def log_name(self):
        return os.path.join(self.save_dir, "metricbeat-logs.{}-{}".format(self.log_info["start_time"], self.log_info["end_time"]))

    def run(self):     
        os.makedirs(os.path.dirname(self.log_name), exist_ok=True)
        with open(self.log_name, 'w') as f:
            f.writelines(self.logs)
        
def run_etl(
    url:str="https://116.101.122.180:5200/metricbeat-*/_search",
    api_key:str=None,
    start_time:str="2024-12-14T00:00:00.000Z", 
    end_time:str="2024-12-15T00:00:00.000Z", 
    step:int=2,
    limit:int=5000,
    cut_off:int=1800 # unit is seconds
):
    
    start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    current_start = start_dt
    while current_start < end_dt:
        current_end = min(current_start + timedelta(seconds=cut_off), end_dt)
        
        new_start_time = current_start.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
        new_end_time = current_end.isoformat(timespec="milliseconds").replace("+00:00", "")+"Z"
        
        print("Getting logs from {} to {}".format(new_start_time, new_end_time))
        
        info = {
            "url":url,
            "api_key":api_key,
            "start_time":new_start_time, 
            "end_time":new_end_time, 
            "step":step,
            "limit":limit
        }
        logs = ExtractMetricBeatLogs(
            **info
        ).get_log()
        logs = Transform(logs=logs).exact_log()
        Load(logs, info, save_dir=f"./logs/{start_time}_{end_time}")
        
        current_start = datetime.strptime(new_start_time, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(seconds=cut_off)
        
    
if __name__ == "__main__":
    
    time_collect = [
        ["2024-12-23T00:00:00.000Z","2024-12-24T00:00:00.000Z"],
        ["2024-12-24T00:00:00.000Z","2024-12-25T00:00:00.000Z"]
    ]
    for time in time_collect:
        run_etl(
            url="https://116.101.122.180:5200/metricbeat-*/_search",
            api_key='',
            start_time=time[0], 
            end_time=time[1], 
            step=1000, # unit is mili seconds
            limit=6000,
            cut_off=900 # unit is seconds
        )
        
# scp -r '/Users/longcaca/Downloads/example' 'aiteam@aiteam:/home/aiteam/Documents/longvu02/'    




#                 jvm_system_cpu_load_1m: {jvm_system_cpu_load_1m:.4f}%, 
#                 jvm_cpu_utilization: {jvm_cpu_utilization:.4f}%,
#                 jvm_system_cpu_utilization: {jvm_system_cpu_utilization:.4f}%, Memory:
#                 jvm.memory.committed: {jvm_memory_committed:.4f}%,
#                 jvm.memory.max: {jvm_memory_max:.4f}%, 
#                 jvm_memory_used: {jvm_memory_used:.4f}%, 
#                 jvm_buffer_memory_used: {jvm_buffer_memory_used:.4f}%,
#                 jvm_memory_usage_after_gc: {jvm_memory_usage_after_gc:.4f}%,
#                 jvm_gc_memory_allocated: {jvm_gc_memory_allocated:.4f}%,
#                 jvm_gc_memory_promoted: {jvm_gc_memory_promoted:.4f}%,
                
#                 process_runtime_jvm_memory_init: {process_runtime_jvm_memory_init:.4f}%,
#                 process_runtime_jvm_memory_limit: {process_runtime_jvm_memory_limit:.4f}%,
#                 process_runtime_jvm_memory_usage: {process_runtime_jvm_memory_usage:.4f}%,
#                 process_runtime_jvm_memory_committed: {process_runtime_jvm_memory_committed:.4f}%,
#                 process_runtime_jvm_memory_usage_after_last_gc: {process_runtime_jvm_memory_usage_after_last_gc:.4f}%,
                
#                 system_memory_utilization: {system_memory_utilization:.4f}%,
#                 system_memory_usage: {system_memory_usage:.4f}%,
#                 latency: {latency:.4f}%,
#                 error_rate: {error_rate:.4f}%,
#                 number_of_requests: {number_of_requests:.4f}%,