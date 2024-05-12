from matplotlib import pyplot as plt
import os
import pandas as pd
from datetime import datetime
import numpy as np

dir = "./part4/results/latest/q1"
percentiles = [5, 10, 50, 67, 75, 80, 85, 90, 95, 99, 999, 9999]
output_dir = "./part4/results/latest/q1"

def transform_cpu_measures(cores: int, run: int):
    file_path = None
    if run == 1:
        file_path = f"{dir}/{cores}_core/t2c{cores}_cpu.txt"
    elif run == 2:
        file_path = f"{dir}/{cores}_core/t2c{cores}_cpu_2.txt"
    data = []
    with open(file_path, "r") as file:
        lines = file.readlines()
        for line in lines:
            entries = line.split(": ")
            entries[1] = entries[1].strip()
            entries[1] = entries[1][1:]
            entries[1] = entries[1][:-1]
            cpu_cores = entries[1].split(", ")
            cpu_cores = np.array(cpu_cores, dtype=float)
            query = {
                "timestamp": entries[0],
                "core_1": float(cpu_cores[0]),
                "core_2": float(cpu_cores[1]),
                "core_3": float(cpu_cores[2]),
                "core_4": float(cpu_cores[3])
            }
            data.append(query)
    df = pd.DataFrame(data)
    if not os.path.exists(f"{dir}/{cores}_core/csv/cpu"):
        os.makedirs(f"{dir}/{cores}_core/csv/cpu")
    if run == 1:
        df.to_csv(f"{output_dir}/{cores}_core/csv/cpu/t2c{cores}_cpu.csv", index=False)
    elif run == 2:
        df.to_csv(f"{output_dir}/{cores}_core/csv/cpu/t2c{cores}_cpu_2.csv", index=False)

def transform_mcperf_measures(cores: int, run: int):
    file_path = None
    if run == 1:
        file_path = f"{dir}/{cores}_core/t2c{cores}_mcperf.txt"
    elif run == 2:
        file_path = f"{dir}/{cores}_core/t2c{cores}_mcperf_2.txt"
    data = []
    with open(file_path, "r") as file:
        lines = file.readlines()
        lines = lines[1:]
        lines = lines[:-2]
        for line in lines:
            line = line.strip()
            entries = line.split()
            query = {
                "avg": float(entries[1]),
                "std": float(entries[2]),
                "min": float(entries[3]),
                "QPS": float(entries[16]),
                "target": float(entries[17]),
                "ts_start": float(entries[18])/1000,
                "ts_end": float(entries[19])/1000,
            }
            index = 4
            for percentile in percentiles:
                query[f"p{percentile}"] = float(entries[index])
                index += 1
            data.append(query)
    df = pd.DataFrame(data)
    if not os.path.exists(f"{dir}/{cores}_core/csv/mcperf"):
        os.makedirs(f"{dir}/{cores}_core/csv/mcperf")
    if run == 1:
        df.to_csv(f"{output_dir}/{cores}_core/csv/mcperf/t2c{cores}_mcperf.csv", index=False)
    elif run == 2:
        df.to_csv(f"{output_dir}/{cores}_core/csv/mcperf/t2c{cores}_mcperf_2.csv", index=False)

def plot(cores: int, run: int):
    transform_cpu_measures(cores, run)
    transform_mcperf_measures(cores,run)
    df_cpu = None
    df_mcperf = None
    if run == 1:
        df_cpu = pd.read_csv(f"{dir}/{cores}_core/csv/cpu/t2c{cores}_cpu.csv")
        df_mcperf = pd.read_csv(f"{dir}/{cores}_core/csv/mcperf/t2c{cores}_mcperf.csv")
    elif run == 2:
        df_cpu = pd.read_csv(f"{dir}/{cores}_core/csv/cpu/t2c{cores}_cpu_2.csv")
        df_mcperf = pd.read_csv(f"{dir}/{cores}_core/csv/mcperf/t2c{cores}_mcperf_2.csv")
    df_mcperf = df_mcperf.sort_values("QPS")
    data = []
    for idx in range(len(df_mcperf)):
        print(df_mcperf.iloc[idx]["avg"])
        df_trimmed = df_cpu[df_cpu["timestamp"] <= df_mcperf.iloc[idx]["ts_end"]]
        df_trimmed = df_trimmed[df_trimmed["timestamp"] >= df_mcperf.iloc[idx]["ts_start"]]
        query = {
            "QPS": df_mcperf.iloc[idx]["QPS"]
        }
        cpu_usages = []
        for idx2 in range(len(df_trimmed)):
            if cores == 1:
                cpu_usages.append(df_trimmed.iloc[idx2]["core_1"])
            elif cores == 2:
                cpu_usages.append(df_trimmed.iloc[idx2]["core_1"] + df_trimmed.iloc[idx2]["core_2"])
        avg_cpu_usage = sum(cpu_usages) / len(cpu_usages)
        query["avg_cpu_usage"] = avg_cpu_usage
        data.append(query)
    df_avg_cpu = pd.DataFrame(data)
    fig = plt.figure(figsize=(16,8))
    fig_ax = fig.gca()
    fig_ax2 = fig_ax.twinx()
    fig_ax.set_xlim(left=0, right=130)
    fig_ax.set_xticks(range(0, 130, 20), labels=(f"{i}k" for i in range(0,130, 20)))
    fig_ax.set_xticks(range(0, 130, 5))
    fig_ax.set_ylim(bottom=0, top=2.2)
    fig_ax.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2], labels=(f"{i/100}" for i in range(0,240,20)))
    fig_ax.set_xlabel("QPS", fontsize=16)
    fig_ax.grid(True)
    fig_ax.set_ylabel("95% latency (ms)", fontsize=16)
    lat_plot, = fig_ax.plot(
        df_mcperf["QPS"]/1000,
        df_mcperf["p95"]/1000,
        label="95% Latency",
        marker="x",
        color="tab:blue",
        markerfacecolor="none",
    )
    slo_line, = fig_ax.plot(
        [0, 130],
        [1,1],
        linestyle=":",
        label="SLO",
        color="tab:red",
        markersize=100
    )
    fig_ax2.set_ylabel("CPU Utilization (%)", fontsize=16)
    cpu_plot, = fig_ax2.plot(
        df_avg_cpu["QPS"]/1000,
        df_avg_cpu["avg_cpu_usage"],
        label="CPU Utilization",
        marker="o",
        color="tab:pink",
        markerfacecolor="none"
    )
    fig_ax.legend([lat_plot, cpu_plot, slo_line], ["95% Latency", "CPU Utilization", "SLO"], loc="lower right", fontsize="large")
    fig.tight_layout()
    if run == 1:
        plt.savefig(f"./part4/results/latest/q1/plots/{cores}_core_plot.pdf")
    elif run == 2:
        plt.savefig(f"./part4/results/latest/q1/plots/{cores}_core_plot_2.pdf")
plot(1,1)
plot(2,1)
plot(1,2)
plot(2,2)