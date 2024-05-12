import psutil
import time
from datetime import datetime

# Function to measure CPU usage
def measure_cpu_usage(interval=1, duration=5, num_cores=2):
    cpu_percentages = []
    start_time = time.time()

    while time.time() - start_time <= duration:
        cpu_percent = psutil.cpu_percent(interval=interval, percpu=True)
        current_usage = 0
        if num_cores == 2:
            current_usage = cpu_percent[0] + cpu_percent[1]
        else:
            current_usage = cpu_percent[0]
        cpu_percentages.append(current_usage)
        time.sleep(interval)

    average_cpu_usage = sum(cpu_percentages) / len(cpu_percentages)
    return average_cpu_usage

# Main function
def main():
    psutil.cpu_percent(interval=None, percpu=True)
    while True:
        cpus = psutil.cpu_percent(interval=None, percpu=True)
        ts = datetime.timestamp(datetime.now())
        print(f"{ts}: {cpus}")
        time.sleep(0.25)

if __name__ == "__main__":
    main()