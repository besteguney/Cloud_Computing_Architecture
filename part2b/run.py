import subprocess
import time
import re

import logging

open("example.log", "w").close()
logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)

# all_test_suites = ["blackscholes", "canneal", "dedup", "ferret", "freqmine", "radix", "vips"]

test_suites = ["blackscholes", "canneal", "dedup", "ferret", "freqmine", "radix", "vips"]

results = {
    "blackscholes": None,
    "canneal": None,
    "dedup": None,
    "ferret": None,
    "freqmine": None,
    "radix": None,
    "vips": None,
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

def duration_to_ms(duration_str):
    parts = duration_str.replace('s', '').split('m')
    
    minutes = float(parts[0])
    seconds = float(parts[1])
    
    total_ms = (minutes * 60 + seconds) * 1000
    return total_ms

def run_test_suite(suite):
    run(["kubectl", "create", "-f", f"parsec-benchmarks/part2b/parsec-{suite}.yaml"])
    job_name = f"parsec-{suite}"
    info(f"Running job {job_name}...")

    info(f"Waiting for {job_name} to be created...")
    time.sleep(5)

    info(f"Waiting for {job_name} to complete...")
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

    results[suite] = real
    
    clear()

def clear():
    run(["kubectl", "delete", "jobs", "--all"])
    run(["kubectl", "delete", "pods", "--all"])

clear()
print()

for suite in test_suites:
    run_test_suite(suite)
    print("\n\n")

clear()

print("RESULTS")
for (key, value) in results.items():
    print(f"{key}: {value}")