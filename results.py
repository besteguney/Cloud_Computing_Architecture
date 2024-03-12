import sys
import pandas as pd
from datetime import datetime

time_format = '%Y-%m-%dT%H:%M:%SZ'
file_path = sys.argv[1]
inference_type = int(sys.argv[2])
data = []
percentiles = [5, 10, 50, 67, 75, 80, 85, 90, 95, 99, 999, 9999]
inferences = ['none', 'cpu', 'l1d', 'l1i', 'l2', 'llc', 'membw']

with open(file_path, 'r') as file:
    next(file)

    for line in file:
        entries = line.split()
        query = {
            'type': entries[0],
            'avg': entries[1],
            'std': entries[2],
            'min': entries[3],
        }
        index = 4
        for percentile in percentiles:
            query[f'p{percentile}'] = entries[index]
            index += 1
        query['QPS'] = entries[16]
        query['target'] = entries[17]
        query['ts_start'] = entries[18]
        query['ts_end'] = entries[19]
        data.append(query)

df = pd.DataFrame(data)
current_time = datetime.now().strftime(time_format)
df.to_csv(f'output_{inferences[inference_type]}_{current_time}.csv')