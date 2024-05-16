import sys
import pandas as pd
import os

folders = [f for f in os.listdir("./part4/q1/")]
percentiles = [5, 10, 50, 67, 75, 80, 85, 90, 95, 99, 999, 9999]
output_dir = "./part4/q1/csv_outputs"

for folder in folders:
    thread_count = folder[1]
    core_count = folder[-1]
    for file_name in os.listdir(f'./part4/q1/{folder}'):
        file_path = f'./part4/q1/{folder}/{file_name}'
        exp_count = file_name[8]
        data = []
        with open(file_path, "r") as file:
            lines = file.readlines()
            lines = lines[:-2]
            lines = lines[1:]
            for line in lines:
                entries = line.split()
                query = {
                    "avg": entries[1],
                    "std": entries[2],
                    "min": entries[3],
                    "ts_start": entries[18],
                    "ts_end": entries[19],
                    "QPS": entries[16],
                    "target": entries[17],
                }
                index = 4
                for percentile in percentiles:
                    query[f'p{percentile}'] = entries[index]
                    index += 1
                data.append(query)
        df = pd.DataFrame(data)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        df.to_csv(f'{output_dir}/t_{thread_count}_c_{core_count}_{exp_count}.csv', index=False)