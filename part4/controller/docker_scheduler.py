import psutil
import subprocess
import docker
from enum import Enum
from scheduler_logger import Job, SchedulerLogger

class ContainerStatus(Enum):
    RESTART="restarting"
    RUN="running"
    PAUSE="paused"
    EXIT="exited"

class DockerScheduler:
    def __init__(self):
        self.docker_client = docker.from_env()
        self.logger_client = SchedulerLogger()
        self.containers = []
        self.current_job = 0
        self.num_cores = 0
        self.images = {
            Job.BLACKSCHOLES: "anakli/cca:parsec_blackscholes",
            Job.CANNEAL: "anakli/cca:parsec_canneal",
            Job.DEDUP: "anakli/cca:parsec_dedup",
            Job.FERRET: "anakli/cca:parsec_ferret",
            Job.FREQMINE: "anakli/cca:parsec_freqmine",
            Job.RADIX: "anakli/cca:splash2x_radix",
            Job.VIPS: "anakli/cca:parsec_vips"
        }

        self.thread_mapping = {
            Job.BLACKSCHOLES: 4,
            Job.CANNEAL: 2,
            Job.DEDUP: 1,
            Job.FERRET: 4,
            Job.FREQMINE: 4,
            Job.RADIX: 4,
            Job.VIPS: 4
        }

        # Initializing some configuration
        # The priorities are determined by checking the CPU usage statistics of part2 of the project.
        # Low priority is also going to run inspired by shortest job first algorithm
        self.low_priority = [Job.BLACKSCHOLES, Job.RADIX]
        self.high_priority = [Job.FREQMINE, Job.FERRET]
        self.middle_priority = [Job.CANNEAL, Job.DEDUP, Job.VIPS]

        self.low_pointer = 0 # Location for RR
        self.high_pointer = 0 # Location for RR
        self.middle_pointer = 0 # Location for not given job

        self.low_running_container = None
        self.high_running_container = None

        self.job_list = [ Job.RADIX, Job.FERRET, Job.FREQMINE, Job.CANNEAL, Job.VIPS, Job.BLACKSCHOLES, Job.DEDUP]
        self.current_container = self.run_or_unpause_container(self.job_list[0], '2-3', 4)

    def get_all_containers(self):
        try:
            return self.docker_client.containers.list(all=True)
        except docker.errors.APIError as e:
            print(f"ERROR: Returning all containers")
            print(e)

    def get_container_status(self, container_name):
        try:
            container = self.docker_client.containers.get(container_name)
            container.reload()
            return container.status
        except docker.errors.NotFound:
            print(f"ERROR: Container not found")

    def get_container_command(self, job: Job, num_threads=1):
        print(f"{job.value}")
        return f"./run -a run -S parsec -p {job.value} -i native -n {num_threads}"

    def get_container(self, job:Job):
        try:
            container = self.docker_client.containers.get(job.value)
            container.reload()
            return container
        except docker.errors.NotFound as e:
            print(f"Container {job.value} not found.")
            print(e)
            return None

    def create_container(self, job: Job, cores, num_threads=1, detach=True, auto_remove=False):
        try:
            container = self.get_container(job)
            #return container
        except docker.errors.NotFound:
            print(f"Container {job.value} does not exist. Creating the container")
        try:
            self.docker_client.images.get(self.images[job])
        except docker.errors.ImageNotFound:
            print(f"Image Not Found: {self.images[job]}. Pulling the image")
            try:
                self.docker_client.images.pull(self.images[job])
            except docker.errors.APIError:
                print("ERROR: Trying to pull image {self.images[job]}")
                return
        try:
            container = self.docker_client.containers.create(cpuset_cpus=cores,
                                             name=job.value,
                                             detach=detach,
                                             auto_remove=auto_remove,
                                             image=self.images[job],
                                             command=self.get_container_command(job, num_threads=num_threads))
            self.containers.append(container)
        except docker.errors.APIError as e:
            print(f"ERROR: trying to crete a container: {job}")
            print(e)
            return
        container.reload()
        return container

    def run_or_unpause_container(self, job:Job, cores, num_threads=1, detach=True, auto_remove=False):
        try:
            container = self.create_container(job=job, cores=cores, num_threads=num_threads, detach=detach, auto_remove=auto_remove)
            container.reload()
        except docker.errors.APIError as e:
            print("ERROR: Trying to run a container {job}")
            print(e)
            return
        if container.status == "running":
            print(f"Container {job.value} already running. Aborting")
            return
        if container.status == "paused":
            self.logger_client.job_unpause(job)
            container.unpause()
        elif container.status == "created":
            self.logger_client.job_start(job, initial_cores=list(cores), initial_threads=num_threads)
            container.start()
        else:
            print(f"Container {job.value} is not suitable for running or unpausing. Current status: {container.status}")
            return
        print(f"Running container {job.value}")
        return container

    def unpause_container(self, job:Job):
        try:
            container = self.get_container(job)
            container.reload()
            if container.status == "running":
                print(f"Container {job.value} already running. Aborting")
                return 
            if container.status == "paused":
                self.logger_client.job_unpause(job)
                print(f"Unpausing container: {job.value}")
                container.unpause()
                return 
            else:
                print(f"Container {job.value} is not suitable for unpausing. Current status: {container.status}")
                return 
        except docker.errors.NotFound:
            print(f"Container {job.value} does not exist. Cannot unpause this container. Aborting")
            return 

    def pause_container(self, job:Job):
        try:
            container = self.get_container(job)
            container.reload()
            if container.status == "running" or container.status == "restarting":
                self.logger_client.job_pause(job)
                container.pause()
        except:
            print(f"ERROR: Trying to pause container {job.value}")
            return

    def update_container(self, job:Job, cores):
        try:
            container = self.get_container(job)
            container.reload()
            if container.status != "exited":
                self.logger_client.update_cores(job, cores=cores)
                container.update(cpuset_cpus=cores)
            else:
                print(f"Container {job.value} already exited.")
                return
        except :
            print(f"ERROR: Trying to update cores of container {job.value}")
            return

    def run_schedule(self, num_cores):
        if self.current_job >= len(self.job_list):
            return
        
        core_list = '1-3' if num_cores == 3 else '2-3'

        if len(self.containers) < self.current_job + 1:
            print(' creating the container ')
            # We need to create the container for the new job
            cur_container = self.run_or_unpause_container(self.job_list[self.current_job], core_list)
            print(f'Status is {cur_container.status}')
            self.containers.append(cur_container)
        elif len(self.containers) == self.current_job + 1:
            print("we are in current t  gfdngdf ")
            # Check if the current container job is finished
            if self.get_container_status(self.job_list[self.current_job].value) != ContainerStatus.EXIT.value:
                # We need to perform an update
                if self.num_cores != num_cores:
                    cur_container = self.containers[self.current_job]
                    self.update_container(self.job_list[self.current_job], core_list)
                    self.num_cores = num_cores
            else:
                self.logger_client.job_end(self.job_list[self.current_job])
                self.current_job = self.current_job + 1
        return
    

    def handle_core_usage_2(self, num_cores):
        if self.current_job >= len(self.job_list):
            return
        
        print('here hbshv ')
        # We are changing the num_cores assigned for batch jobs.
        if self.num_cores != num_cores:
            print('it is chaninggg ')
            # At the start we still need to assign high priority to the last cores
            if self.num_cores == 0:
                cur_high_job = self.high_priority[0]
                self.run_or_unpause_container(cur_high_job, cores='2-3', num_threads=self.thread_mapping[cur_high_job])
                self.high_running_container = self.get_container(cur_high_job)

            self.num_cores = num_cores
            # We are increasing the number of cores for the batch jobs
            if num_cores == 3:
                # Get the container for lower priority back.
                cur_low_job = self.low_priority[0]
                self.run_or_unpause_container(cur_low_job, cores='1', num_threads=self.thread_mapping[cur_low_job])
            # We are decreasing the number of cores for the batch jobs.
            else:
                cur_low_job = self.low_priority[0]
                if self.get_container(cur_low_job) != None:
                    self.pause_container(cur_low_job)

        # At the end we need to check if any of the jobs are done
        high_run_container = None if len(self.high_priority) <= 0 else self.get_container(self.high_priority[0])
        if high_run_container != None and high_run_container.status == ContainerStatus.EXIT.value:
            print(f'{self.high_priority[0].value} is endingg ')
            self.current_job += 1
            if len(self.high_priority) == 1:
                print('jdsjfgds high priority 1')
                self.high_priority = []
            else:
                print('jdsjfgds high priority 2')
                self.high_priority = [self.high_priority[1]]

            # We need to assign from the middle priority jobs
            if self.middle_pointer < len(self.middle_priority):
                new_job = self.middle_priority[self.middle_pointer]
                self.high_priority.append(new_job)
                self.middle_pointer += 1
                print('increased the middle high')

            if len(self.high_priority) > 0:
                cur_high_job = self.high_priority[0]
                self.run_or_unpause_container(cur_high_job, cores='2-3', num_threads=self.thread_mapping[cur_high_job])

        low_run_container = None if len(self.low_priority) <= 0 else self.get_container(self.low_priority[0])
        if low_run_container != None and low_run_container.status == ContainerStatus.EXIT.value:
            print(f'{self.low_priority[0].value} is endingg ')
            self.current_job += 1
            if len(self.low_priority) == 1:
                print('jdsjfgds low priority 1')
                self.low_priority = []
            else:
                print('jdsjfgds low priority 2')
                self.low_priority = [self.low_priority[1]]

            # We need to assign from the middle priority jobs
            if self.middle_pointer < len(self.middle_priority):
                new_job = self.middle_priority[self.middle_pointer]
                self.low_priority.append(new_job)
                self.middle_pointer += 1
                print('increased the middle lowww')

            if num_cores == 3 and len(self.low_priority) > 0:
                cur_low_job = self.low_priority[0]
                self.run_or_unpause_container(cur_low_job, cores='1', num_threads=self.thread_mapping[cur_low_job])


        # If any of them is empty we need to give all the cores to the others
        if len(self.low_priority) == 0:
            if len(self.high_priority) != 0 and num_cores == 3:
                high_job = self.high_priority[0]
                self.update_container(high_job, '1-3')

        if len(self.high_priority) == 0:
            if len(self.low_priority) != 0:
                low_job = self.low_priority[0]
                cores = '1-3' if num_cores == 3 else '2-3'
                if self.get_container(self.low_priority[0]) != None:
                    print(f'udpating the cores {num_cores}')
                    self.update_container(low_job, cores)
                else:
                    self.run_or_unpause_container(self.low_priority[0], cores=cores, num_threads=self.thread_mapping[self.low_priority[0]])

    
    def handle_core_usage(self, num_cores):
        if self.is_schedule_done():
            return
        
        if num_cores == 3:
            self.update_container(self.job_list[self.current_job], '1-3')
        else:
            self.update_container(self.job_list[self.current_job], '2-3')
        

        if self.current_container.status == ContainerStatus.EXIT.value:
            self.current_job += 1
            cores = '2-3' if num_cores == 2 else '1-3'
            if self.current_job < 7:
                self.current_container = self.run_or_unpause_container(self.job_list[self.current_job], cores)
        
    # Removes all the containers.
    def remove_containers(self):
        for container in self.containers:
            container.remove(force=True)
    
    def is_schedule_done(self)->bool:
        return self.current_job >= len(self.job_list)
    
