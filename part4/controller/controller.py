from memcache_scheduler import MemcacheScheduler
from scheduler_logger import Job, SchedulerLogger
from scheduler import Scheduler
from collections import deque
from time import sleep

def main():
    #scheduler = Scheduler()
    memcache_scheduler = MemcacheScheduler()
    scheduler = Scheduler()

    while True:
        #print('icerde')
        available_cores = memcache_scheduler.adapt_cpu_usage()
        scheduler.run_schedule(available_cores)
        sleep(0.5)

if __name__ == "__main__":
    main()