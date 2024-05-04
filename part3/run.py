import datetime
import logging
import re
import subprocess
import time


MEMCACHED = "memcached"
BLACKSCHOLES = "parsec-blackscholes"
CANNEAL = "parsec-canneal"
DEDUP = "parsec-dedup"
FERRET = "parsec-ferret"
RADIX = "parsec-radix"
VIPS = "parsec-vips"
FREQMINE = "parsec-freqmine"

ALL_JOBS = [BLACKSCHOLES, CANNEAL, DEDUP, FERRET, RADIX, VIPS, FREQMINE]

def get_yaml(job):
    return job + ".yaml"

RUN = 3
RUN_DIR = f"RUN{RUN}"
LOG_FILE = f"{RUN_DIR}/stdout.log"
PODS_OUT = f"{RUN_DIR}/results.json"

root = logging.getLogger()
root.setLevel(logging.DEBUG)

# stdout_handler = logging.StreamHandler()
# stdout_handler.setLevel(logging.DEBUG)
# root.addHandler(stdout_handler)

file_handler = logging.FileHandler(filename=LOG_FILE, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
root.addHandler(file_handler)

def info(msg):
    print(f"INFO: {msg}")

# 2-CORE NODE = [
#     [BLACKSCHOLES]
# ]

# 8-CORE NODE = [
#     [FREQMINE, (VIPS, RADIX)]
# ]

# 4-CORE NODE = [
#     [CANNEAL, DEDUP]
#     [FERRET]
# ]

dependents = {
    FREQMINE: [VIPS, RADIX],
    CANNEAL: [DEDUP],
}

start_jobs = [BLACKSCHOLES, FREQMINE, CANNEAL, FERRET]

def run(cmd, log=True):
    out = subprocess.run(cmd, capture_output=True)
    stdout = out.stdout.decode('unicode_escape').strip()
    stderr = out.stderr.decode('unicode_escape').strip()
    if log and out.stdout:
        logging.info(f"CMD OUT: {stdout}")

    if log and out.stderr:
        logging.error(f"CMD ERR: {stderr}")

    return stdout

def clear():
    run(["kubectl", "delete", "jobs", "--all"])

class JobScheduler:
    def __init__(self) -> None:
        self.finished_jobs = []
        self.iter_counter = 0

        for job in start_jobs:
            self.create_job(job)

    def create_job(self, job):
        info(f"Creating job {job} at {datetime.datetime.now()}")
        run(["kubectl", "create", "-f", get_yaml(job)])

    def finish_job(self, job):
        print(f"Job {job} finished at {datetime.datetime.now()}.")
        self.finished_jobs.append(job)
        if job in dependents:
            for dependent in dependents[job]:
                self.create_job(dependent)

    def check(self):
        if self.iter_counter % 10 == 0:
            # Just to get some info in the log
            run(["kubectl", "get", "jobs"])
            run(["kubectl", "get", "pods", "-o", "wide"])

        pod_stats = get_pod_stats()
        is_finished = True

        for (job, status) in pod_stats.items():
            if status: 
                if job not in self.finished_jobs:
                    self.finish_job(job)
            else:
                is_finished = False

        self.iter_counter += 1
        
        return is_finished


def get_pod_stats():
    pod_stats = {}
    out = run(["kubectl", "get", "jobs"], log=False)

    for job in ALL_JOBS:
        match = re.findall(re.compile(job + r"\s+(\d+)\/"), out)
        if len(match) > 0:
            pod_stats[job] = int(match[0]) == 1
        else:
            pod_stats[job] = False
    
    return pod_stats    

start = datetime.datetime.now()
info(f"Starting at {start}")

job_scheduler = JobScheduler()
while not job_scheduler.check():
    time.sleep(1)

end = datetime.datetime.now()
info(f"Finished at {end} with duration {end - start}")

with open(PODS_OUT, 'w+') as f:
    subprocess.run(["kubectl", "get", "pods", "-o", "json"], stdout=f)

clear()