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

class Scheduler:
    def __init__(self):
        self.docker_client = docker.from_env()
        self.logger_client = SchedulerLogger()
        self.containers = []
        self.current_job = 0
        self.num_cores = 3
        self.images = {
            Job.BLACKSCHOLES: "anakli/cca:parsec_blackscholes",
            Job.CANNEAL: "anakli/cca:parsec_canneal",
            Job.DEDUP: "anakli/cca:parsec_dedup",
            Job.FERRET: "anakli/cca:parsec_ferret",
            Job.FREQMINE: "anakli/cca:parsec_freqmine",
            Job.RADIX: "anakli/cca:splash2x_radix",
            Job.VIPS: "anakli/cca:parsec_vips"
        }

        self.job_list = [Job.BLACKSCHOLES, Job.CANNEAL, Job.DEDUP, Job.FERRET, Job.FREQMINE, Job.RADIX, Job.VIPS]

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
            return

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
                container.unpause()
            else:
                print(f"Container {job.value} is not suitable for unpausing. Current status: {container.status}")
                return
            print(f"Unpausing container: {job.value}")
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
                self.current_job = self.current_job + 1
        return
    
""" scheduler = Scheduler()
scheduler.pause_container(Job.FERRET)
scheduler.unpause_container(Job.FERRET)
scheduler.update_container(Job.FERRET, cores="0-2") """
