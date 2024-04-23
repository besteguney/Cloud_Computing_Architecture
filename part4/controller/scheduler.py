import psutil
import subprocess
import docker
from enum import Enum
from scheduler_logger import Job
from scheduler_logger import SchedulerLogger

class ContainerStatus(Enum):
    RESTART="RESTARTING"
    RUN="RUNNING"
    PAUSE="PAUSING"
    EXIT="EXITING"

class Scheduler:
    def __init__(self):
        self.docker_client = docker.from_env()
        self.logger_client = SchedulerLogger()
        self.running_containers = []
        self.images = {
            Job.BLACKSCHOLES: "anakli/cca:parsec_blackscholes",
            Job.CANNEAL: "anakli/cca:parsec_canneal",
            Job.DEDUP: "anakli/cca:parsec_dedup",
            Job.FERRET: "anakli/cca:parsec_ferret",
            Job.FREQMINE: "anakli/cca:parsec_freqmine",
            Job.RADIX: "anakli/cca:splash2x_radix",
            Job.VIPS: "anakli/cca:parsec_vips"
        }

    def get_all_containers(self):
        try:
            return self.docker_client.containers.list(all=True)
        except docker.errors.APIError as e:
            print(f"ERROR: Returning all containers")
            print(e)

    def get_container_status(self, container_name):
        try:
            return self.docker_client.containers.get(container_name).status
        except docker.errors.NotFound:
            print(f"ERROR: Container not found")

    def get_container_command(self, job: Job, num_threads=1):
        print(f"{job.value}")
        return f"./run -a run -S parsec -p {job.value} -i native -n {num_threads}"

    def create_container(self, job: Job, cores, num_threads=1, detach=True, auto_remove=False):
        try:
            container = self.docker_client.containers.get(job.value)
            return container
        except docker.errors.NotFound:
            print(f"Container {job.value} does not exist. Creating the container")
            pass
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
            self.logger_client.job_start(job, initial_cores=list(cores), initial_threads=list(num_threads))
            container.start()
        else:
            print(f"Container {job.value} is not suitable for running or unpausing. Current status: {container.status}")
            return
        print("Running container {job.value}")
        return

scheduler = Scheduler()
scheduler.run_or_unpause_container(Job.BLACKSCHOLES, cores="0")
