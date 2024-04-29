from memcache_handler import MemcacheHandler
from scheduler_logger import SchedulerLogger

import argparse
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--interval", type=float, default=0.3)
    parser.add_argument("-n", "--amt", type=int, default=2)
    parser.add_argument("-u", "--high_mode_threshold", type=float, default=90)
    parser.add_argument("-l", "--low_mode_threshold", type=float, default=140)

    args = parser.parse_args()

    memcache_handler = MemcacheHandler(
        logger=SchedulerLogger(),
        high_threshold=args.high_mode_threshold,
        low_threshold=args.low_mode_threshold
    )
    memcache_handler.run_docker_scheduler()
    memcache_handler.run()


if __name__ == "__main__":
    main()