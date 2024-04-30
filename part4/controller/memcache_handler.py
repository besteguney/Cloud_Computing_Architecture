import psutil
from time import sleep
import subprocess
from scheduler_logger import Job, SchedulerLogger

class MemcacheHandler:
    def __init__(self, scheduler_logger: SchedulerLogger):
        self.process_id = self.get_process_id()
        print(f'current process is {self.process_id}')
        self.scheduler_logger = scheduler_logger
        self.cpu_list = [0,1]

        # Initialize memcache.
        self.set_cpu_affinity("0-1")
        print(' jdhfdjshf jfdsjfjds dhfjsd ')
        self.scheduler_logger.job_start(Job.MEMCACHED, ['0-1'], 2)
    
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
        if len(self.cpu_list) == 2 and current_usage <= 75:
            available_cores = 3
            self.set_cpu_affinity("0")
            self.scheduler_logger.update_cores(Job.MEMCACHED, ['0'])
            self.cpu_list = [0]
        elif len(self.cpu_list) == 1 and current_usage > 80:
            available_cores = 2
            self.set_cpu_affinity("0-1")
            self.scheduler_logger.update_cores(Job.MEMCACHED, ['0-1'])
            self.cpu_list = [0, 1]

        return available_cores
    
#scheduler = MemcacheScheduler()
#scheduler.set_cpu_affinity("0-2")
#print(scheduler.get_cpu_utilization(False, 0))
#print(psutil.cpu_percent(interval=None, percpu=True)[0])
