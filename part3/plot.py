import json
import datetime
import re
from statistics import mean, stdev
import csv
import matplotlib.pyplot as plt
import numpy as np

MEMCACHED = "memcached"
BLACKSCHOLES = "parsec-blackscholes"
CANNEAL = "parsec-canneal"
DEDUP = "parsec-dedup"
FERRET = "parsec-ferret"
RADIX = "parsec-radix"
VIPS = "parsec-vips"
FREQMINE = "parsec-freqmine"
TOTAL = "total"

time_format = '%Y-%m-%dT%H:%M:%SZ'

mcperf = [[], [], []]

runtimes = {
    BLACKSCHOLES: [],
    CANNEAL: [],
    DEDUP: [],
    FERRET: [],
    RADIX: [],
    VIPS: [],
    FREQMINE: [],
    TOTAL: []
}

for i in range(1, 4):
    run_dir = f"RUN{i}"
    pods_file = run_dir + f"/pods_{i}.json"

    with open(pods_file) as f:
        json_file = json.load(f)

        start_times = []
        completion_times = []

        for item in json_file['items']:
            name = item['status']['containerStatuses'][0]['name']
            if str(name) != "memcached":
                # need to add 2 hours because of time difference
                start_time = datetime.datetime.strptime(
                    item['status']['containerStatuses'][0]['state']['terminated']['startedAt'],
                    time_format) + datetime.timedelta(hours=2)
                completion_time = datetime.datetime.strptime(
                        item['status']['containerStatuses'][0]['state']['terminated']['finishedAt'],
                        time_format) + datetime.timedelta(hours=2)
                start_times.append(start_time)
                completion_times.append(completion_time)
                runtimes[name].append((start_time, completion_time, (completion_time - start_time).seconds))

        total_start = min(start_times)
        total_end = max(completion_times)
        runtimes[TOTAL].append((total_start, total_end, (total_end - total_start).seconds))

    mperf_file = run_dir + f"/mcperf_{i}.txt"

    with open(mperf_file) as f:
        reader = csv.reader(f, delimiter=' ', skipinitialspace=True)
        next(reader, None)
        start_date = None
        for row in reader:
            p95_latency = float(row[12])
            start = datetime.datetime.fromtimestamp(float(row[18]) / 1000)
            end = datetime.datetime.fromtimestamp(float(row[19]) / 1000)

            mcperf[i - 1].append((p95_latency, start, end))

means = {}
stds = {}

for (job, timings) in runtimes.items():
    raw_timings = [t[2] for t in timings]
    m = mean(raw_timings)
    s = stdev(raw_timings)

    means[job] = m
    stds[job] = s

def print_table():
    table = f"""
    \\begin{{table}}[ht]
        \\centering
        \\begin{{tabular}}{{ |c|c|c|c|c|}} 
            \\hline
            job name & mean time [s] & std [s] \\\\ \\hline \\hline
            \\coloredcell{{blackscholes}}    & {means[BLACKSCHOLES]:.1f} & {stds[BLACKSCHOLES]:.1f} \\\\  \\hline
            \\coloredcell{{canneal}}         & {means[CANNEAL]:.1f} & {stds[BLACKSCHOLES]:.1f} \\\\   \\hline
            \\coloredcell{{dedup}}           & {means[DEDUP]:.1f} & {stds[DEDUP]:.1f} \\\\  \\hline
            \\coloredcell{{ferret}}          & {means[FERRET]:.1f} & {stds[FERRET]:.1f} \\\\  \\hline
            \\coloredcell{{freqmine}}        & {means[FREQMINE]:.1f} & {stds[FREQMINE]:.1f} \\\\   \\hline
            \\coloredcell{{radix}}           & {means[RADIX]:.1f} & {stds[RADIX]:.1f} \\\\  \\hline
            \\coloredcell{{vips}}           & {means[VIPS]:.1f} & {stds[VIPS]:.1f} \\\\  \\hline
            total time      & {means[TOTAL]:.1f} & {stds[TOTAL]:.1f} \\\\ \\hline
        \\end{{tabular}}
    \\end{{table}}
"""
    print(table)

def create_figures():
    
    for i in range(0, 3):
        start = runtimes[TOTAL][i][0].timestamp()
        x_vals = []
        widths = []
        y_vals = [m[0] for m in mcperf[i]]

        for entry in mcperf[i]:
            x_vals.append(entry[1].timestamp() - start)
            widths.append(entry[2].timestamp() - entry[1].timestamp())

        fig, (ax2, ax1) = plt.subplots(2, 1, sharex=True)
        fig.set_size_inches(10.5, 5.5)
        fig.suptitle(f'Run {i + 1}', fontsize=22)
        ax1.bar(x_vals, y_vals, widths, align='edge')

        ax1.set_ylabel('p95 latency (ms)')
        ax1.set_xlabel('Time (s)')

        # Fixing random state for reproducibility
        # Example data
        nodes = ["node-b-2", "node-b-4", "node-e-8"]

        blackscholes_start = runtimes[BLACKSCHOLES][i][0].timestamp() - runtimes[TOTAL][i][0].timestamp()
        ax2.barh(0, width=runtimes[BLACKSCHOLES][i][2], height=0.25, left=blackscholes_start, color="#CCA000", label="blackscholes")

        freqmine_start = runtimes[FREQMINE][i][0].timestamp() - runtimes[TOTAL][i][0].timestamp()
        ax2.barh(2, width=runtimes[FREQMINE][i][2], height=0.25, left=freqmine_start, color="#0CCA00", label="freqmine")

        vips_start = runtimes[VIPS][i][0].timestamp() - runtimes[TOTAL][i][0].timestamp()
        ax2.barh(2 - 1/16, width=runtimes[VIPS][i][2], height=0.125, left=vips_start, color="#CC0A00", label="vips")

        radix_start = runtimes[RADIX][i][0].timestamp() - runtimes[TOTAL][i][0].timestamp()
        ax2.barh(2 + 1/16, width=runtimes[RADIX][i][2], height=0.125, left=radix_start, color="#00CCA0", label="radix")

        ferret_start = runtimes[FERRET][i][0].timestamp() - runtimes[TOTAL][i][0].timestamp()
        ax2.barh(1 + 1/16, width=runtimes[FERRET][i][2], height=0.125, left=ferret_start, color="#AACCCA", label="ferret")

        canneal_start = runtimes[CANNEAL][i][0].timestamp() - runtimes[TOTAL][i][0].timestamp()
        ax2.barh(1 - 1/16, width=runtimes[CANNEAL][i][2], height=0.125, left=canneal_start, color="#CCCCAA", label="canneal")

        dedup_start = runtimes[DEDUP][i][0].timestamp() - runtimes[TOTAL][i][0].timestamp()
        ax2.barh(1 - 1/16, width=runtimes[DEDUP][i][2], height=0.125, left=dedup_start, color="#CCACCA", label="dedup")

        ax2.set_yticks([0, 1, 2], labels=nodes)
        ax2.tick_params(axis=u'both', which=u'both',length=0)
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.spines['bottom'].set_visible(False)
        ax2.spines['left'].set_visible(False)
        ax2.set_aspect(20)

        fig.legend(ncol=1, loc="upper right")
        plt.savefig(f"p3_run{i}.pdf")

create_figures()