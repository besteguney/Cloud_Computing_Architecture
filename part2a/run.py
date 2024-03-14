import subprocess
import time
import re
from pprint import pprint

import logging

open("example.log", "w").close()
logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)

# all_test_suites = ["blackscholes", "canneal", "dedup", "ferret", "freqmine", "radix", "vips"]
# all_interferences = ["none", "cpu", "l1d", "l1i", "l2", "llc", "membw"]

test_suites = ["blackscholes", "canneal", "dedup", "ferret", "freqmine", "radix", "vips"]
interferences = ["none", "cpu", "l1d", "l1i", "l2", "llc", "membw"]

results = {
    "blackscholes": [],
    "canneal": [],
    "dedup": [],
    "ferret": [],
    "freqmine": [],
    "radix": [],
    "vips": [],
}

def info(msg):
    print(f"INFO: {msg}")

def run(cmd):
    out = subprocess.run(cmd, capture_output=True)
    stdout = out.stdout.decode('unicode_escape').strip()
    stderr = out.stderr.decode('unicode_escape').strip()
    if out.stdout:
        logging.info(f"CMD OUT: {stdout}")

    if out.stderr:
        logging.error(f"CMD ERR: {stderr}")

    return stdout

def setup_bench_interference(interference):
    info(f"Setting up bench interference {interference}...")
    run(["kubectl", "create", "-f", f"interference/ibench-{interference}.yaml"])
    info("Sleeping for 15 seconds to ensure interference is set up properly...\n\n")
    time.sleep(15)

def delete_bench_interference(interference):
    pod_name = f"ibench-{interference}"
    info(f"Deleting bench interference {interference}")
    run(["kubectl", "delete", "pod", pod_name])

def duration_to_ms(duration_str):
    parts = duration_str.replace('s', '').split('m')
    
    minutes = float(parts[0])
    seconds = float(parts[1])
    
    total_ms = (minutes * 60 + seconds) * 1000
    return total_ms

def run_test_suite(suite, interference):
    if interference != "none":
        setup_bench_interference(interference)

    run(["kubectl", "create", "-f", f"parsec-benchmarks/part2a/parsec-{suite}.yaml"])
    job_name = f"parsec-{suite}"
    info(f"Running job {job_name} with interference {interference}...")

    pods_out = run(["kubectl", "get", "pods"])

    info(f"Waiting for {job_name} with {interference} to be created...")
    time.sleep(5)

    info(f"Waiting for {job_name} with {interference} complete...")
    while True:
        pods_out = run(["kubectl", "get", "jobs"])
        done = int(re.findall(re.compile(f"{job_name}\s+(\d)\/1"), pods_out)[0])
        if done:
            break
        time.sleep(10)

    info("Job completed! Extracting measurements...")
    pod_id = run(["kubectl", "get", "pods", f"--selector=job-name={job_name}", f"--output=jsonpath='{{.items[*].metadata.name}}'"])[1:-1]
    logs = run(["kubectl", "logs", pod_id])

    real = re.findall(re.compile(f"real\s+(\d+m\d+.\d+s)"), logs)[0]
    user = re.findall(re.compile(f"user\s+(\d+m\d+.\d+s)"), logs)[0]
    sys = re.findall(re.compile(f"sys\s+(\d+m\d+.\d+s)"), logs)[0]
    print(f"Real: {real}, User: {user}, Sys: {sys}")

    results[suite].append(real)
    
    clear()

def clear():
    run(["kubectl", "delete", "jobs", "--all"])
    run(["kubectl", "delete", "pods", "--all"])

clear()
print()

for interference in interferences:
    print(f"Starting jobs with interference {interference}\n")
    for suite in test_suites:
        run_test_suite(suite, interference)

    print("\n\n")

clear()

print("RAW RESULTS")
for (key, value) in results.items():
    print(f"{key}: {value}")

normalized_results = {}

for (key, value) in results.items():
    normalized = []
    val = None
    for i in range(0, len(value)):
        if i == 0:
            normalized.append(1)
            val = duration_to_ms(value[0])
        else:
            normalized.append(duration_to_ms(value[i]) / val)

    normalized_results[key] = normalized

print("NORMALIZED RESULTS")
for (key, value) in normalized_results.items():
    print(f"{key}: {value}")