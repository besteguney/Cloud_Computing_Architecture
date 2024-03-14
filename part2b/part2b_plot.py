import os
import pandas as pd
import matplotlib.pyplot as plt

DIR = "./part2b/data.csv"
WORKLOADS = ["blackscholes", "canneal", "dedup", "ferret", "freqmine", "radix", "vips"]
MARKERS = ["o", "v", "s", "*", "x", "d", "P"]
THREADS = [1, 2, 4, 8]
fig = plt.figure(figsize=(14, 10))
ax = fig.gca()

# Linear Speedup Line
ax.errorbar(x=range(0, 9),y=range(0, 9),label="Linear Speedup",linestyle="--",marker=".", markersize=8, capsize=2)
df = pd.read_csv(DIR, sep=";", header=0)
for marker_idx, workload in enumerate(WORKLOADS):
    curr_df = df[df["workload"] == workload]
    y_values = [curr_df.iloc[0]["1"] / curr_df.iloc[0][f"{num_thread}"] for num_thread in THREADS]
    for num_thread in THREADS:
        curr_df.iloc[0][f"{num_thread}"] = curr_df.iloc[0][f"{num_thread}"] / curr_df.iloc[0]["1"]
    ax.errorbar(x=THREADS, y=y_values, label=workload, marker=MARKERS[marker_idx], markersize=8, capsize=4)


ax.set_xlabel("Number of Threads")
ax.set_ylabel("Normalized Speedup")
ax.legend(loc="upper right", fontsize=14)
ax.grid(True, color="lightgray", linestyle="--", linewidth=1)
ax.tick_params(labelsize=12)
ax.set_xlim(left=0, right=9)
ax.set_ylim(bottom=0, top=9)
ax.set_yticks(range(0, 9, 1))
ax.set_xticks([1, 2, 4, 8])
if not os.path.exists("./part2b/plots/"):
    os.makedirs("./part2b/plots")
plt.savefig("./part2b/plots/part2b.png")