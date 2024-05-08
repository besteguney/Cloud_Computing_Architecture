import psutil
import time

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
    interval = 1  
    duration = 5 
    
    while True:
        average_cpu = measure_cpu_usage(interval, duration)
        print(f"Average CPU usage over {duration} seconds: {average_cpu:.2f}%")

if __name__ == "__main__":
    main()