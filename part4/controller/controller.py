from memcache_handler import MemcacheHandler
from scheduler_logger import SchedulerLogger
from docker_scheduler import DockerScheduler
from memcache_handler import MemcacheMode
from time import sleep
import argparse
import random

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--interval", type=float, default=0.3)
    parser.add_argument("-n", "--amt", type=int, default=2)
    parser.add_argument("-u", "--high_mode_threshold", type=float, default=90)
    parser.add_argument("-l", "--low_mode_threshold", type=float, default=100)

    args = parser.parse_args()

    logger = SchedulerLogger()
    """
    memcache_handler = MemcacheHandler(
        logger=SchedulerLogger(),
        high_threshold=args.high_mode_threshold,
        low_threshold=args.low_mode_threshold
    )
    """
    scheduler = DockerScheduler(
        scheduler_logger=logger
    )
    scheduler.create_all_containers()
    while not scheduler.is_schedule_done():
        available_cores = random.choice([1,2])
        print(f"Cores is {available_cores}")
        scheduler.handle_cores(available_cores)
        sleep(0.25)

    scheduler.remove_containers()
    #memcache_handler.set_cpu_affinity("0-1")

if __name__ == "__main__":
    main()