import psutil
from time import sleep
import subprocess
from scheduler_logger import Job, SchedulerLogger
from enum import Enum
from time import time
class MemcacheMode(Enum):
    TWO_CORE_MODE = 0
    ONE_CORE_MODE = 1

class MemcacheHandler:

    def __init__(self, logger:SchedulerLogger, high_threshold=80, low_threshold=180):
        self.process_id = self.get_process_id()
        print(f'Memcahed PID is {self.process_id}')
        self.logger = logger
        self.cpu_list = [0-1]
        self.set_cpu_affinity("0")
        print(f"Memcached CPU affinity set to 0-1")
        self.logger.job_start(Job.MEMCACHED, ['0-1'], 2)
        self.mode = MemcacheMode.TWO_CORE_MODE
        self.high_threshold = high_threshold
        self.low_threshold = low_threshold
        self.memc_process = psutil.Process(self.process_id)
        self.last_transition = 0

    def get_process_id(self)->int:
        pid = None
        for process in psutil.process_iter(['pid', 'name']):
            if 'memcached' in process.info['name']:
                pid = process.info['pid']
                break

        return -1 if pid is None else int(pid)

    def set_cpu_affinity(self, core_list):
        if self.process_id == -1:
            # Log that the process is not running
            return

        command = ['sudo', 'taskset', '-acp', core_list, str(self.process_id)]
        try:
            subprocess.run(command, check=True)
            # Logging the details
            print(f"Set CPU affinity for PID {self.process_id} to CPU(s) {core_list}")
            cpus = core_list.split('-')
            if len(cpus) == 1:
                self.cpu_list = [0]
            else:
                self.cpu_list = [0,1]
        except subprocess.CalledProcessError as e:
            #Logging the details
            print(f"Error setting CPU affinity: {e}")

    # Followed the steps in https://rosettacode.org/wiki/Linux_CPU_utilization
    def get_cpu_utilization(self, all=True, cpu_no=0)->float:
        count = 0
        with open('/proc/stat', 'r') as file:
            for line in file:
                if not all and count != cpu_no+1:
                    count = count + 1
                    continue
                line = line.strip()[5:].split(' ')
                total_time = 0
                for item in line:
                    total_time = total_time + int(item)
                idle_fraction = float(line[2]) / total_time
                return 100 * (1 - idle_fraction)

    # This method monitors the cpu usage and adapts the cores.
    def adapt_cpu_usage(self) -> int:
        cpu_utilizations = psutil.cpu_percent(interval=None, percpu=True)
        current_usage = 0
        available_cores = 4 - len(self.cpu_list)
        if len(self.cpu_list) == 1:
            current_usage = cpu_utilizations[self.cpu_list[0]]
        elif len(self.cpu_list) == 2:
            current_usage = cpu_utilizations[self.cpu_list[0]] + cpu_utilizations[self.cpu_list[1]]
            current_usage = current_usage / 2

        # If we are using 2 core and average CPU usage is <= 50% assign one core
        if len(self.cpu_list) == 2 and current_usage <= 100:
            available_cores = 3
            self.set_cpu_affinity("0")
            self.logger.update_cores(Job.MEMCACHED, ['0'])
            self.cpu_list = [0]
        elif len(self.cpu_list) == 1 and current_usage > 90:
            available_cores = 2
            self.set_cpu_affinity("0-1")
            self.logger.update_cores(Job.MEMCACHED, ['0-1'])
            self.cpu_list = [0, 1]

        return available_cores

    def switch_to_two_core_mode(self):
        self.mode = MemcacheMode.TWO_CORE_MODE
        print(f"Switching to HIGH QPS MODE. Cores for Memcache: 0-1")
        self.logger.update_cores(Job.MEMCACHED, ["0", "1"])
        self.set_cpu_affinity("0-1")

    def switch_to_one_core_mode(self):
        self.mode = MemcacheMode.ONE_CORE_MODE
        print(f"Switching to LOW QPS MODE. Cores: 0")
        self.logger.update_cores(Job.MEMCACHED, ["0"])
        self.set_cpu_affinity("0")

    def run(self):
        cpu_utilizations = psutil.cpu_percent(interval=4, percpu=True)
        memcache_usage = 0
        for + in range(10):
            q = psutil.cpu_percent(interval=4, percpu=True)
            usage = q[0] + q[1] if self.mode == MemcacheMode.TWO_CORE_MODE else q[0]
            if usage > memcache_usage:
                memcache_usage = usage
        """
        if self.mode == MemcacheMode.TWO_CORE_MODE:
            memcache_usage = cpu_utilizations[0] + cpu_utilizations[1]
        else:
            memcache_usage = cpu_utilizations[0]
        """
        now = time()
        if self.mode == MemcacheMode.ONE_CORE_MODE:
            if memcache_usage >= self.high_threshold:
                self.switch_to_two_core_mode()
                return 2
            return 3
        elif self.mode == MemcacheMode.TWO_CORE_MODE:
            if memcache_usage <= self.low_threshold and now - self.last_transition > 4:
                self.last_transition = now
                self.switch_to_one_core_mode()
                return 3
            return 2

