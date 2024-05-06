from memcache_handler import MemcacheHandler
from scheduler_logger import SchedulerLogger
from docker_scheduler import DockerScheduler
from memcache_handler import MemcacheMode
from time import sleep
import argparse
import random
from scheduler_logger import Job


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--interval", type=float, default=0.3)
    parser.add_argument("-n", "--amt", type=int, default=2)
    parser.add_argument("-u", "--high_mode_threshold", type=float, default=70)
    parser.add_argument("-l", "--low_mode_threshold", type=float, default=100)

    args = parser.parse_args()

    logger = SchedulerLogger()

    scheduler = DockerScheduler(
        scheduler_logger=logger
    )
    memcache_handler = MemcacheHandler(
        logger=logger,
        high_threshold=args.high_mode_threshold,
        low_threshold=args.low_mode_threshold
    )
    available_cores = 2
    while not scheduler.is_schedule_done():
        available_cores = memcache_handler.run()
        scheduler.handle_cores(available_cores)
        sleep(0.5)

    logger.end()

if __name__ == "__main__":
    main()
