import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

DIR = "./part1/csv_outputs/"
MEASUREMENT_TYPES = ["no_intf", "ibench_cpu", "ibench_l1d", "ibench_l1i", "ibench_l2", "ibench_llc", "ibench_membw"]
MARKERS = ["o", "v", "s", "*", "x", "d", "P"]
RUNS = 3

fig = plt.figure(figsize=(14,10))
ax = fig.gca()

for marker_idx, type in enumerate(MEASUREMENT_TYPES):
    tail_arr = np.ndarray(shape=(11, 3), dtype=float)
    qps_arr = np.ndarray(shape=(11,3), dtype=float)
    for i in range(RUNS):
        index = i+1
        file_path = f'{DIR}/{type}_{index}.csv'
        curr_df = pd.read_csv(file_path, sep=",", header=0)
        arr = curr_df[["p95", "QPS"]].to_numpy()
        for j in range(len(arr)):
            tail_arr[j][i] = arr[j][0] / 1000
            qps_arr[j][i] = arr[j][1]
    # Array is the final array that holds the p95 Mean, p95 SEM, QPS Mean and QPS STD values
    arr = np.ndarray(shape=(11, 4), dtype=float)
    for i in range(len(tail_arr)):
        arr[i][0] = np.mean(tail_arr[i])
        arr[i][1] = stats.sem(tail_arr[i])
        arr[i][2] = np.mean(qps_arr[i])
        arr[i][3] = stats.tstd(qps_arr[i])
    file_df = pd.DataFrame(arr, columns=["p95 Mean", "p95 SEM", "QPS Mean", "QPS STD"])
    file_df["Type"] = type
    ax.errorbar(x=file_df["QPS Mean"], y=file_df["p95 Mean"], xerr=file_df["QPS STD"], yerr=file_df["p95 SEM"], label=type, marker=MARKERS[marker_idx], markersize=8, capsize=2)

ax.set_xlabel("Mean Queries per Second (QPS)", fontsize=16)
ax.set_ylabel("95th Percentile Latency in Miliseconds (ms)", fontsize=16)
ax.legend(loc='upper right', fontsize=14)
ax.grid(True, color='lightgray', linestyle='--', linewidth=1)
ax.tick_params(labelsize=12)
ax.set_xlim(left=0, right=55100)
ax.set_ylim(bottom=0, top=10)
ax.set_yticks(range(0, 11, 2))
ax.set_xticks(range(0, 55100, 5000))
if not os.path.exists("./part1/plots/"):
    os.makedirs("./part1/plots")
plt.savefig("./part1/plots/part1.png")