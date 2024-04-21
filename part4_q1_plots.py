import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

dir = "./part4/q1/csv_outputs"
thread_core_counts = ["t_1_c_1", "t_1_c_2", "t_2_c_1", "t_2_c_2"]
markers = ["o", "v", "s", "*"]
runs = 3

fig = plt.figure(figsize=(28,10))
ax = fig.gca()
ax.set_title("Latency by QPS averaged over 3 runs", fontsize=20)

for marker_idx, type in enumerate(thread_core_counts):
    tail_arr = np.ndarray(shape=(25,3), dtype=float)
    qps_arr = np.ndarray(shape=(25,3), dtype=float)
    for i in range(runs):
        index = i+1
        file_path = f'{dir}/{type}_{index}.csv'
        curr_df = pd.read_csv(file_path, sep=",", header=0)
        arr = curr_df[["p95", "target"]].to_numpy()
        for j in range(len(arr)):
            tail_arr[j][i] = arr[j][0] / 1000
            qps_arr[j][i] = arr[j][1]
    arr = np.ndarray(shape=(25,4), dtype=float)
    for i in range(len(tail_arr)):
        arr[i][0] = np.mean(tail_arr[i])
        arr[i][1] = stats.sem(tail_arr[i])
        arr[i][2] = np.mean(qps_arr[i])
        arr[i][3] = stats.tstd(qps_arr[i])
    file_df = pd.DataFrame(arr, columns=["p95 Mean", "p95 SEM", "QPS Mean", "QPS STD"])
    file_df["Type"] = type
    ax.errorbar(x=file_df["QPS Mean"], y=file_df["p95 Mean"], xerr=file_df["QPS STD"], yerr=file_df["p95 SEM"], label=type, marker=markers[marker_idx], markersize=8, capsize=2)

ax.set_xlabel("Mean Queries per Second (QPS)", fontsize=16, labelpad=10)
ax.set_ylabel("95th Percentile Latency in Miliseconds (ms)", fontsize=16)
ax.legend(loc='upper right', fontsize=14)
ax.grid(True, color='lightgray', linestyle='--', linewidth=1)
ax.tick_params(labelsize=18)
ax.set_xlim(left=0, right=125000)
ax.set_ylim(bottom=0, top=2.125)
ax.set_xticks(range(0, 125001, 20000), labels=(f'{i}k' for i in range(0,121, 20)))
if not os.path.exists("./part4/q1/plots/"):
    os.makedirs("./part4/q1/plots")
plt.savefig("./part4/q1/plots/part4_q1.pdf")
