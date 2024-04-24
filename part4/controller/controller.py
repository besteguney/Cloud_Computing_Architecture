from memcache_handler import MemcacheHandler
from scheduler_logger import Job, SchedulerLogger
from docker_scheduler import DockerScheduler
from collections import deque
from time import sleep

def main():
    memcache_scheduler = MemcacheHandler()
    docker_scheduler = DockerScheduler()

    while not docker_scheduler.is_schedule_done():
        available_cores = memcache_scheduler.adapt_cpu_usage()
        docker_scheduler.handle_core_usage_2(available_cores)
        sleep(1)
    
    docker_scheduler.remove_containers()
    memcache_scheduler.set_cpu_affinity('0-1')

if __name__ == "__main__":
    main()