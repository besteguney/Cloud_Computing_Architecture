import dateutil.parser
from matplotlib import pyplot as plt
import os
import pandas as pd
from datetime import datetime
import dateutil
import numpy as np

dir = "./part4/results/latest/q4/smallest_interval/"
percentiles = [5, 10, 50, 67, 75, 80, 85, 90, 95, 99, 999, 9999]
output_dir = "./part4/results/latest/q4/smallest_interval/plots"

def plotA(run: int):
    file_path = f"{dir}/mcperf/txt/smallest_interval_12_{run}.txt"
    data = []
    with open(file_path, "r") as file:
        lines = file.readlines()
        start_time = None
        end_time = None
        for line in lines:
            if line.startswith("Timestamp start:"):
                start_time = float(line.split(": ")[1].strip()) / 1000
            if line.startswith("Timestamp end:"):
                end_time = float(line.split(": ")[1].strip()) / 1000
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
            query["timestamp"] = idx*12
            data.append(query)
    df = pd.DataFrame(data)
    if not os.path.exists(f"{dir}/mcperf/csv"):
        os.makedirs(f"{dir}/mcperf/csv")
    df.to_csv(f"{dir}/mcperf/csv/smallest_interval_mcperf_{run}.csv", index=False)
    violation_ration = len(
            df['p95'][df['p95'] > 1000]) / len(df['p95']) * 100
    file_path = f"{dir}/jobs/txt/smallest_interval_12_{run}_job.txt"
    data = []
    with open(file_path, "r") as file:
        lines = file.readlines()
        for line in lines:
            entries = [entry.strip() for entry in line.split(" ")]
            entries[0] = datetime.timestamp(dateutil.parser.parse(entries[0]))
            query = {
                "timestamp": entries[0],
                "event": entries[1],
                "job": entries[2]
            }
            data.append(query)
    df_log = pd.DataFrame(data)
    df_log["timestamp"] -= df_log["timestamp"][0]
    if not os.path.exists(f"{dir}/jobs/csv"):
        os.makedirs(f"{dir}/jobs/csv")
    df_log.to_csv(f"{dir}/jobs/csv/smallest_interval_jobs_{run}.csv", index=False)
    fig = plt.figure(figsize=(16,8))
    lat_ax, job_ax = fig.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]})
    lat_ax.set_xlim(left=0, right=900)
    lat_ax.set_xlabel("Time (s)")
    lat_ax.grid(True)
    lat_ax.set_ylabel("QPS")
    qps_plot = lat_ax.bar(
        df["timestamp"],
        df["QPS"]/1000,
        width=5,
        label="QPS",
        color="papayawhip",
        zorder=2,
        align="edge"
    )
    lat_ax.set_yticks(range(0, 105, 25), labels=(f"{i}k" for i in range(0,105,25)))
    lat_ax2 = lat_ax.twinx()
    lat_ax.yaxis.tick_right()
    lat_ax.yaxis.set_label_position("right")
    lat_ax2.yaxis.tick_left()
    lat_ax2.yaxis.set_label_position("left")
    lat_ax2.set_ylabel("95%-tile Latency (in ms)")
    p95_plot, = lat_ax2.plot(
        [t + 5 for t in df["timestamp"]],
        df["p95"] / 1000,
        label="95\% Latency",
        marker="x",
        markersize=4,
        markerfacecolor='none',
        color="tab:blue",
        zorder=3
    )
    slo_line,=lat_ax2.plot(
        [0,1000],
        [1,1],
        linestyle=":",
        label="SLO",
        color="tab:red",
        markersize=100,
    )
    lat_ax.legend([p95_plot, slo_line, qps_plot], ["95%-tile", "SLO", "QPS"])

    jobs = ["vips", "blackscholes", "canneal", "dedup", "radix", "ferret", "freqmine"]
    colors = {
        "vips": "#cc0a00",
        "blackscholes": "#cca000",
        "canneal": "#cccaaa",
        "dedup": "#ccacca",
        "radix": "#00cca0",
        "ferret": "#aaccca",
        "freqmine": "#0cca00"
    }
    df_jobs = df_log[df_log["job"].isin(jobs)]
    job_ax.set_yticks(range(0,7))
    job_ax.set_yticklabels(jobs)
    job_ax.set_ylim([-1, 7])
    job_ax.set_xlim([0, 950])
    job_ax.grid(True)
    for idx, name in enumerate(jobs):
        entries = df_jobs[df_jobs["job"] == name]
        entries = entries[entries["event"].isin(["start", "pause", "unpause", "end"])]
        line = None
        if len(entries) == 2:
            job_ax.plot([entries.iloc[0]["timestamp"], entries.iloc[-1]["timestamp"]], [idx, idx], color=colors[name], linewidth=5, label=name)
        else:
            for i in range(0, len(entries), 2):
                job_ax.plot([entries.iloc[i]["timestamp"], entries.iloc[i+1]["timestamp"]], [idx, idx], color=colors[name], linewidth=5, label=name)
    fig.tight_layout()
    plt.savefig(f"./part4/results/latest/q4/smallest_interval/plots/smallest_interval_plot_{run}_A.pdf")

def plotB(run: int):
    if not os.path.exists(f"{dir}/mcperf/csv") or not os.path.exists(f"{dir}/jobs/txt"):
        raise ValueError("CSV file must exist")
    file_path = f"{dir}/mcperf/csv/smallest_interval_mcperf_{run}.csv"
    file_path_log = f"{dir}/jobs/txt/smallest_interval_12_{run}_job.txt"
    df = pd.read_csv(file_path)
    data = []
    with open(file_path_log, "r") as file:
        lines = file.readlines()
        for line in lines:
            entries = [entry.strip() for entry in line.split(" ")]
            if entries[2] != "memcached":
                continue
            else:
                entries[0] = datetime.timestamp(dateutil.parser.parse(entries[0]))
                query = {
                    "timestamp": entries[0],
                    "event": entries[1],
                    "cores": 1 if entries[3] == "[0]" else 2
                }
                data.append(query)
    data.append({
        "timestamp": data[0]["timestamp"]+900,
        "event": None,
        "cores": 2
    })
    df_cores = pd.DataFrame(data)
    df_cores["timestamp"] -= df_cores.iloc[0]["timestamp"]
    fig = plt.figure(figsize=(16,8))
    lat_ax, job_ax = fig.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]})
    lat_ax.set_title(f"4.4 {run}B",
                     fontsize=14, fontweight="bold", pad=10)
    lat_ax.set_xlim(left=0, right=900)
    lat_ax.set_xlabel("Time (s)")
    lat_ax.grid(True)
    lat_ax.set_ylabel("QPS")
    qps_plot = lat_ax.bar(
        df["timestamp"],
        df["QPS"] / 1000,
        width=12,
        align="edge",
        label="QPS",
        color="lightsteelblue",
        zorder=3,
    )
    lat_ax.set_yticks(range(0, 105, 25), labels=(f"{i}k" for i in range(0,105,25)))
    lat_ax2 = lat_ax.twinx()
    lat_ax.yaxis.tick_right()
    lat_ax.yaxis.set_label_position("right")
    lat_ax2.yaxis.tick_left()
    lat_ax2.yaxis.set_label_position("left")
    lat_ax2.set_ylabel("Number of cores")
    lat_ax2.set_ylim([0,2.5])
    lat_ax2.set_yticks([0, 0.5, 1, 1.5, 2, 2.5])
    cores_plot, = lat_ax2.plot(df_cores["timestamp"], df_cores["cores"], drawstyle="steps-post", color="red")
    """
    cores_plot = lat_ax2.bar(
        df_cores["timestamp"],
        df_cores["cores"],
        label="Number of cores",
        width=10,
        align="edge",
        color="papayawhip",
        zorder=1,
    )
    """
    lat_ax.legend([qps_plot, cores_plot], ["QPS", "number of cores"])
    jobs = ["vips", "blackscholes", "canneal", "dedup", "radix", "ferret", "freqmine"]
    colors = {
        "vips": "#cc0a00",
        "blackscholes": "#cca000",
        "canneal": "#cccaaa",
        "dedup": "#ccacca",
        "radix": "#00cca0",
        "ferret": "#aaccca",
        "freqmine": "#0cca00"
    }
    df_log=pd.read_csv(f"{dir}/jobs/csv/smallest_interval_jobs_{run}.csv")
    df_jobs = df_log[df_log["job"].isin(jobs)]
    job_ax.set_yticks(range(0,7))
    job_ax.set_yticklabels(jobs)
    job_ax.set_ylim([-1, 7])
    job_ax.set_xlim([0, 900])
    job_ax.grid(True)
    for idx, name in enumerate(jobs):
        entries = df_jobs[df_jobs["job"] == name]
        entries = entries[entries["event"].isin(["start", "pause", "unpause", "end"])]
        line = None
        if len(entries) == 2:
            job_ax.plot([entries.iloc[0]["timestamp"], entries.iloc[-1]["timestamp"]], [idx, idx], color=colors[name], linewidth=5, label=name)
        else:
            for i in range(0, len(entries), 2):
                job_ax.plot([entries.iloc[i]["timestamp"], entries.iloc[i+1]["timestamp"]], [idx, idx], color=colors[name], linewidth=5, label=name)
    fig.tight_layout()
    plt.savefig(f"./part4/results/latest/q4/smallest_interval/plots/smallest_interval_plot_{run}_B.pdf")

plotA(1)
plotB(1)
plotA(2)
plotB(2)
plotA(3)
plotB(3)