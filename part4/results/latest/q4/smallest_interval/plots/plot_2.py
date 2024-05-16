import dateutil.parser
from matplotlib import pyplot as plt
import os
import pandas as pd
from datetime import datetime
import dateutil
import numpy as np

dir = "./part4/results/latest/q4/smallest_interval/"
percentiles = [5, 10, 50, 67, 75, 80, 85, 90, 95, 99, 999, 9999]
output_dir = "./part4/results/latest/q4/smallest_interval/"

def plotA(run: int):
    file_path = f"{dir}/mcperf/txt/smallest_interval_12_{run}.txt"
    data = []
    with open(file_path, "r") as file:
        lines = file.readlines()
        start_time = None
        end_time = None
        for line in lines:
            if line.startswith("Timestamp start:"):
                start_time = int(line.split(": ")[1].strip()) / 1000
            if line.startswith("Timestamp end:"):
                end_time = int(line.split(": ")[1].strip()) / 1000
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
    if not os.path.exists(f"{output_dir}/mcperf/csv"):
        os.makedirs(f"{output_dir}/mcperf/csv")
    df.to_csv(f"{output_dir}/mcperf/csv/smallest_interval_mcperf_{run}.csv", index=False)
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
    if not os.path.exists(f"{output_dir}/jobs/csv"):
        os.makedirs(f"{output_dir}/jobs/csv")
    df_log.to_csv(f"{output_dir}/jobs/csv/smallest_interval_jobs_{run}.csv", index=False)
    fig = plt.figure(figsize=(16,10))
    lat_ax, job_ax = fig.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]})
    lat_ax.set_xlim(left=0, right=800)
    lat_ax.set_xticks(range(0, 800, 100), labels=(f"{i}" for i in range(0, 800, 100)))
    lat_ax.set_xticks(range(0, 800, 50))
    lat_ax.set_xlabel("Time (s)", fontsize=18, fontweight="semibold", labelpad=10)
    lat_ax.grid(True)
    lat_ax.set_ylabel("QPS", fontsize=18, fontweight="semibold")
    lat_ax.set_ylim(bottom=0, top=110000)
    lat_ax.set_yticks(range(0, 110000, 20000), labels=(f"{i}k" for i in range(0, 110, 20)))
    lat_ax.set_yticks(range(0, 110000, 10000))
    qps_plot = lat_ax.bar(
        df["timestamp"],
        df["QPS"],
        width=10,
        label="QPS",
        color="navajowhite",
        zorder=2,
        align="edge"
    )
    lat_ax2 = lat_ax.twinx()
    lat_ax.yaxis.tick_right()
    lat_ax.yaxis.set_label_position("right")
    lat_ax2.yaxis.tick_left()
    lat_ax2.yaxis.set_label_position("left")
    lat_ax2.set_ylabel("95th Percentile Latency (ms)", fontsize=18, fontweight="semibold")
    lat_ax2.set_ylim(bottom=0, top=2.2)
    lat_ax2.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0])
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
    lat_ax.legend([p95_plot, slo_line, qps_plot], ["95th Percentile", "SLO", "QPS"], fontsize=18)

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
    plots = {
        "vips": None,
        "blackscholes": None,
        "canneal": None,
        "dedup": None,
        "radix": None,
        "ferret": None,
        "freqmine": None
    }
    df_jobs = df_log[df_log["job"].isin(jobs)]
    job_ax.set_yticks(range(0,7))
    job_ax.set_yticklabels(jobs)
    job_ax.set_ylim([-1, 7])
    job_ax.set_xlim(left=0, right=800)
    job_ax.set_xticks(range(0, 800, 100), labels=(f"{i}" for i in range(0, 800, 100)))
    job_ax.set_xticks(range(0, 800, 50))
    job_ax.tick_params(top=True, labeltop=True, bottom=False, labelbottom=False)
    job_ax.grid(True)
    for idx, name in enumerate(jobs):
        entries = df_jobs[df_jobs["job"] == name]
        entries = entries[entries["event"].isin(["start", "pause", "unpause", "end"])]
        print(entries["timestamp"])
        line = None
        if len(entries) == 2:
            plots[name], = job_ax.plot([entries.iloc[0]["timestamp"], entries.iloc[-1]["timestamp"]], [idx, idx], color=colors[name], linewidth=5, label=name)
        else:
            for i in range(0, len(entries), 2):
                plots[name], = job_ax.plot([entries.iloc[i]["timestamp"], entries.iloc[i+1]["timestamp"]], [idx, idx], color=colors[name], linewidth=5, label=name)
    job_ax.legend(plots.values(), plots.keys())
    fig.tight_layout()
    plt.savefig(f"./part4/results/latest/q4/smallest_interval/plots/smallest_interval_{run}_A.pdf")

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
    fig = plt.figure(figsize=(16,10))
    lat_ax, job_ax = fig.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]})
    lat_ax.set_xlim(left=0, right=800)
    lat_ax.set_xticks(range(0, 800, 100), labels=(f"{i}" for i in range(0, 800, 100)))
    lat_ax.set_xticks(range(0, 800, 50))
    lat_ax.set_xlabel("Time (s)", fontsize=18, fontweight="semibold", labelpad=10)
    lat_ax.grid(True)
    lat_ax.set_ylabel("QPS", fontsize=18, fontweight="semibold")
    lat_ax.set_ylim(bottom=0, top=110000)
    lat_ax.set_yticks(range(0, 110000, 20000), labels=(f"{i}k" for i in range(0, 110, 20)))
    lat_ax.set_yticks(range(0, 110000, 10000))
    qps_plot = lat_ax.bar(
        df["timestamp"],
        df["QPS"],
        width=10,
        align="edge",
        label="QPS",
        color="lightsteelblue",
        zorder=3,
    )
    lat_ax2 = lat_ax.twinx()
    lat_ax.yaxis.tick_right()
    lat_ax.yaxis.set_label_position("right")
    lat_ax2.yaxis.tick_left()
    lat_ax2.yaxis.set_label_position("left")
    lat_ax2.set_ylabel("Number of cores", fontsize=18, fontweight="semibold")
    lat_ax2.set_ylim([0,2.2])
    lat_ax2.set_yticks([0, 1, 2])
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
    lat_ax.legend([qps_plot, cores_plot], ["QPS", "Number of cores"], fontsize=18, loc="lower right")
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
    job_ax.set_xlim(left=0, right=800)
    job_ax.set_xticks(range(0, 800, 100), labels=(f"{i}" for i in range(0, 800, 100)))
    job_ax.set_xticks(range(0, 800, 50))
    job_ax.tick_params(top=True, labeltop=True, bottom=False, labelbottom=False)
    job_ax.grid(True)
    plots = {
        "vips": None,
        "blackscholes": None,
        "canneal": None,
        "dedup": None,
        "radix": None,
        "ferret": None,
        "freqmine": None
    }
    for idx, name in enumerate(jobs):
        entries = df_jobs[df_jobs["job"] == name]
        entries = entries[entries["event"].isin(["start", "pause", "unpause", "end"])]
        line = None
        if len(entries) == 2:
            plots[name], = job_ax.plot([entries.iloc[0]["timestamp"], entries.iloc[-1]["timestamp"]], [idx, idx], color=colors[name], linewidth=5, label=name)
        else:
            for i in range(0, len(entries), 2):
                plots[name], = job_ax.plot([entries.iloc[i]["timestamp"], entries.iloc[i+1]["timestamp"]], [idx, idx], color=colors[name], linewidth=5, label=name)
    job_ax.legend(plots.values(), plots.keys())
    fig.tight_layout()
    plt.savefig(f"./part4/results/latest/q4/smallest_interval/plots/smallest_interval_{run}_B.pdf")
plotA(1)
plotA(2)
plotA(3)
plotB(1)
plotB(2)
plotB(3)