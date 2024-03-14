import sys
import pandas as pd
import os

files = [f for f in os.listdir("./part1/outputs")]
measurement_types = {
    "0": "no_intf",
    "1": "ibench_cpu",
    "2": "ibench_l1d",
    "3": "ibench_l1i",
    "4": "ibench_l2",
    "5": "ibench_llc",
    "6": "ibench_membw",
}
percentiles = [5, 10, 50, 67, 75, 80, 85, 90, 95, 99, 999, 9999]
output_directory = "./part1/csv_outputs"
for file in files:
    data = []
    measure_int = file[7]
    measurement_type = measurement_types[measure_int]
    measurement_index = file[9]
    file_path = f'./part1/outputs/{file}'
    with open(file_path, "r") as file:
        lines = file.readlines()
        lines = lines[:-2]
        lines = lines[1:]
        for line in lines:
            entries = line.split()
            query = {
                "type": entries[0],
                "avg": entries[1],
                "std": entries[2],
                "min": entries[3],
            }
            index = 4
            for percentile in percentiles:
                query[f'p{percentile}'] = entries[index]
                index += 1
            query["QPS"] = entries[16]
            query["target"] = entries[17]
            data.append(query)
    df = pd.DataFrame(data)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    df.to_csv(f'./part1/csv_outputs/{measurement_type}_{measurement_index}.csv', index = False)

