from matplotlib import pyplot as plt
import os
import pandas as pd

dir = "./part4/results/latest/q3/plots/mcperf/txt"
percentiles = [5, 10, 50, 67, 75, 80, 85, 90, 95, 99, 999, 9999]
output_dir = "./part4/results/latest/q3/plots/mcperf/csv"

def plotA(run: int):
    file_path = f"{dir}/mcperf_{run}.txt"
    data = []
    with open(file_path, "r") as file:
        lines = file.readlines()
        start_time = None
        for line in lines:
            if line.startswith("Timestamp start:"):
                start_time = int(line.split(": ")[1].strip()) / 1000
                print(start_time)
                break
        lines = lines[7:]
        lines = lines[:-11]
        for idx, line in enumerate(lines):
            entries = line.split()
            query = {
                "avg": float(entries[1]),
                "std": float(entries[2]),
                "min": float(entries[3]),
                "QPS": float(entries[16]),
                "target": float(entries[17]),
            }
            index = 4
            for percentile in percentiles:
                query[f"p{percentile}"] = float(entries[index])
                index += 1
            query["timestamp"] = idx*10
            data.append(query)
    df = pd.DataFrame(data)
    print(df["timestamp"])
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    df.to_csv(f"{output_dir}/mcperf_{run}.csv", index=False)
    violation_ration = len(
            df['p95'][df['p95'] > 1000]) / len(df['p95']) * 100
    print(violation_ration)
    fig = plt.figure(figsize=(10,5))
    fig_ax2 = fig.gca()
    fig_ax = fig_ax2.twinx()
    fig_ax.yaxis.tick_left()
    fig_ax2.yaxis.tick_right()
    fig_ax.yaxis.set_label_position("left")
    fig_ax2.yaxis.set_label_position("right")
    fig_ax2.bar(
        df["timestamp"],
        df["QPS"]/1000,
        width=10,
        label="QPS",
        color="lightsteelblue",
        zorder=2,
        align="edge"
    )
    fig_ax.plot(
        [t + 5 for t in df["timestamp"]],
        df["p95"] / 1000,
        label="95\% Latency",
        marker="x",
        markersize=4,
        markerfacecolor='none',
        color="tab:blue",
        zorder=3
    )
    fig_ax.plot(
        [0,1000],
        [1,1],
        linestyle=":",
        label="SLO",
        color="tab:gray"
    )
    print(df["timestamp"][89] - df["timestamp"][0])
    fig_ax.set_title(f"4.3 {run}A",
                     fontsize=14, fontweight="bold", pad=10)
    fig_ax2.set_xlabel("Time (s)", fontsize=14)
    fig_ax2.set_ylim(bottom=-5, top=120)
    fig_ax2.set_xlim(left=0, right=1000)
    fig_ax2.set_ylabel("QPS", fontsize=14)
    fig_ax2.set_yticks(range(0, 105, 25), labels=(
            f'{i}k' for i in range(0, 105, 25)))
    fig_ax.grid(True, color='lightgray', linestyle='--', linewidth=1)
    fig_ax.set_ylabel("95%-tile time (in ms)", fontsize=14)
    fig_ax.set_ylim(bottom=0.01, top=1.8)
    fig_ax.set_yticks([i / 5 + 0.2 for i in range(9)])
    fig.legend(
            loc='lower right', fontsize=9.5, bbox_to_anchor=(0.92, 0.12))
    plt.tight_layout()
    plt.savefig(f"./part4/results/latest/q3/plots/plot4_{run}_A.pdf")

plotA(1)
plotA(2)
plotA(3)