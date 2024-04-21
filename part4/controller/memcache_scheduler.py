import psutil
import subprocess

class MemcacheScheduler:
    def __init__(self):
        self.process_id = self.get_process_id()
        self.cpu_list = []
        return
    
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
            self.cpu_list = [item for item in range(int(cpus[0]), int(cpus[1])+1)]
            print(self.cpu_list)
        except subprocess.CalledProcessError as e:
            #Logging the details
            print(f"Error setting CPU affinity: {e}")
    
    # Followed the steps in https://rosettacode.org/wiki/Linux_CPU_utilization
    def get_cpu_utilization(self)->float:
        with open('/proc/stat', 'r') as file:
            for line in file:
                line = line.strip()[5:].split(' ')
                total_time = 0
                for item in line:
                    total_time = total_time + int(item)
                idle_fraction = float(line[3]) / total_time
                return 100 * (1 - idle_fraction)

scheduler = MemcacheScheduler()
scheduler.set_cpu_affinity("0-2")
print(scheduler.get_cpu_utilization())
