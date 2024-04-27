import docker
from enum import Enum
from scheduler_logger import Job, SchedulerLogger

# This enum shows that status of the container. Taken from https://docker-py.readthedocs.io/en/stable/containers.html.
class ContainerStatus(Enum):
    RESTART="restarting"
    RUN="running"
    PAUSE="paused"
    EXIT="exited"

# Scheduler class to handle the batch jobs.
class DockerScheduler:
    def __init__(self, scheduler_logger: SchedulerLogger):
        self.docker_client = docker.from_env()
        self.logger_client = scheduler_logger
        self.containers = [] # Holds the created containers for the batch jobs.
        self.current_job = 0
        self.num_cores = 2 # Current core usage of batch jobs.
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

        self.job_list = [ Job.RADIX, Job.FERRET, Job.FREQMINE, Job.CANNEAL, Job.VIPS, Job.BLACKSCHOLES, Job.DEDUP]
        self.current_container = self.run_or_unpause_container(self.job_list[0], '2-3', 4)
        self.logger_client.job_start(self.job_list[0], '2-3', 4)

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
            container.start()
            self.current_container = container
            #print(f'started the container {self.job_list[self.current_job]}')
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

    def handle_core_usage(self, num_cores):
        if self.is_schedule_done():
            return
        
        # Checking the current status of the container.
        status = self.get_container_status(self.job_list[self.current_job].value)
        
        # Checking if the current job is ending.
        if status == ContainerStatus.EXIT.value:
            self.logger_client.job_end(self.job_list[self.current_job])
            self.current_job += 1
            cores = '2-3' if num_cores == 2 else '1-3'
            if self.current_job < len(self.job_list):
                number_of_threads = self.thread_mapping[self.job_list[self.current_job]]
                self.current_container = self.run_or_unpause_container(self.job_list[self.current_job], cores, num_threads=number_of_threads)
                self.logger_client.job_start(self.job_list[self.current_job], [item for item in range(int(cores.split('-')[0]), int(cores.split('-')[1])+1)], number_of_threads)
        # If we are updating the number of cores.
        elif num_cores != self.num_cores:
            if num_cores == 3:
                self.update_container(self.job_list[self.current_job], '1-3')
                self.logger_client.update_cores(self.job_list[self.current_job], ['1', '2', '3'])
            else:
                self.update_container(self.job_list[self.current_job], '2-3')
                self.logger_client.update_cores(self.job_list[self.current_job], ['2', '3'])
            self.num_cores = num_cores
        
        
    # Removes all the containers.
    def remove_containers(self):
        for container in self.containers:
            container.remove(force=True)
    
    # Checking if the schedule is done.
    def is_schedule_done(self)->bool:
        return self.current_job >= len(self.job_list)