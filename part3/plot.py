import json
from datetime import datetime
import re
from statistics import mean, stdev
import csv

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

mcperf = []

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
                start_time = datetime.strptime(
                    item['status']['containerStatuses'][0]['state']['terminated']['startedAt'],
                    time_format)
                completion_time = datetime.strptime(
                        item['status']['containerStatuses'][0]['state']['terminated']['finishedAt'],
                        time_format)
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
            start = datetime.fromtimestamp(float(row[18]) / 1000)
            end = datetime.fromtimestamp(float(row[19]) / 1000)

            mcperf.append((p95_latency, start, end))

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

print_table()