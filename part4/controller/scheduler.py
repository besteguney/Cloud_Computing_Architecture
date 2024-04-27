import psutil
import subprocess
import docker
from enum import Enum
from scheduler_logger import Job
from scheduler_logger import SchedulerLogger
from time import sleep, time
from scheduler_exceptions import ContainerNotFound, ImageNotFound
class ContainerStatus(Enum):
    RESTART="RESTARTING"
    RUN="RUNNING"
    PAUSE="PAUSING"
    EXIT="EXITING"

HIGH_QPS_MODE = 0
LOW_QPS_MODE = 1
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
        self.mode = HIGH_QPS_MODE
        self.current_job: Job = None
        self.high_qps_cores = "0,1"
        self.low_qps_cores = "0,1,2"
        self.order = [Job.FERRET, Job.VIPS, Job.FREQMINE, Job.RADIX, Job.DEDUP, Job.BLACKSCHOLES, Job.CANNEAL]
        self.container_start_end = {}
        self.total = {}

    def get_all_containers(self):
        try:
            containers = self.docker_client.containers.list(all=True)
            return containers
        except docker.errors.APIError as e:
            print(f"ERROR: Returning all containers")
            print(e)
            raise e

    def get_all_images(self):
        return self.docker_client.images.list(all=True)

    def get_container_status(self, job:Job):
        try:
            container = self.docker_client.containers.get(job.value)
            container.reload()
            return container.status
        except docker.errors.NotFound as e:
            print(f"ERROR: Container not found")
            raise e

    def get_container_command(self, job: Job, num_threads=1):
        return f"./run -a run -S parsec -p {job.value} -i native -n {num_threads}"

    def get_container(self, job:Job):
        try:
            container = self.docker_client.containers.get(job.value)
            container.reload()
            return container
        except docker.errors.NotFound as e:
            print(f"Container {job.value} not found.")
            return False

    def get_image(self, job:Job):
        try:
            image = self.docker_client.images.get(f"anakli/cca:parsec_{job.value}")
            return image
        except docker.errors.ImageNotFound:
            print(f"Image Not Found for {job.value}. Pulling the image")
            return False

    def remove_container(self, job:Job):
        try:
            container = self.get_container(job)
            container.remove()
        except docker.errors.APIError as e:
            print(f"ERROR: Trying to remove container {job.value}")
            return False

    def create_container(self, job: Job, cores, num_threads=1, detach=True, auto_remove=False):
        if self.get_container(job):
            return self.get_container(job)
        else:
            print(f"Container {job.value} does not exist. Creating the container")
            if not self.get_image(job):
                print(f"Image Not Found: {self.images[job]}. Pulling the image")
                try:
                    self.docker_client.images.pull(self.images[job])
                except:
                    print("ERROR: Trying to pull image {self.images[job]}")
            else:
                container = self.docker_client.containers.create(cpuset_cpus=cores,
                                                name=job.value,
                                                detach=detach,
                                                auto_remove=auto_remove,
                                                image=self.images[job],
                                                command=self.get_container_command(job, num_threads=num_threads))
                container.reload()
                return container

    def run_or_unpause_container(self, job:Job, cores, num_threads=1, detach=True, auto_remove=False):
        try:
            container = self.create_container(job=job, cores=cores, num_threads=num_threads, detach=detach, auto_remove=auto_remove)
            container.reload()
        except docker.errors.APIError as e:
            print("ERROR: Trying to run a container {job}")
            print(e)
            raise e
        else:
            if container.status == "running":
                print(f"Container {job.value} already running. Aborting")
                return
            if container.status == "paused":
                self.logger_client.job_unpause(job)
                container.unpause()
            elif container.status == "created":
                self.logger_client.job_start(job, initial_cores=cores.split(","), initial_threads=int(num_threads))
                container.start()
            else:
                print(f"Container {job.value} is not suitable for running or unpausing. Current status: {container.status}")
                return
            print("Running container {job.value}")

    def unpause_container(self, job:Job):
        try:
            container = self.get_container(job)
        except docker.errors.NotFound as e:
            print(f"Container {job.value} does not exist. Cannot unpause this container. Aborting")
            raise e
        else:
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

    def pause_container(self, job:Job):
        try:
            container = self.get_container(job)
        except:
            print(f"ERROR: Trying to pause container {job.value}")
            raise Exception("ERROR while pausing the container {job.value}")
        else:
            container.reload()
            if container.status == "running" or container.status == "restarting":
                self.logger_client.job_pause(job)
                container.pause()

    def update_container(self, job:Job, cores):
        try:
            container = self.get_container(job)
        except :
            print(f"ERROR: Trying to update cores of container {job.value}")
            raise Exception("Error while updating the container {job.value}")
        else:
            container.reload()
            if container.status != "exited":
                self.logger_client.update_cores(job, cores=cores)
                container.update(cpuset_cpus=cores)
            else:
                print(f"Container {job.value} already exited.")
                return

    def wait_container(self, job:Job):
        try:
            container = self.get_container(job)
        except:
            print(f"ERROR: Waiting for the container {job.value}")
            raise Exception("Error while waiting for the container {job.value}")
        else:
            container.reload()
            return container.wait()

    def set_high_qps_mode(self):
        if self.mode != HIGH_QPS_MODE:
            self.mode = HIGH_QPS_MODE
            try:
                self.update_container(self.current_job, self.high_qps_cores)
                self.logger_client.update_cores(self.current_job, cores=self.high_qps_cores.split(","))
            except:
                print(f"ERROR: Trying to set the high qps mode on container {self.current_job.value}")
                raise Exception("Error while setting the high qps mode on container {self.current_job.value}")

    def set_low_qps_mode(self):
        if self.mode != LOW_QPS_MODE:
            self.mode = LOW_QPS_MODE
            try:
                self.update_container(self.current_job, self.low_qps_cores)
                self.logger_client.update_cores(self.current_job, cores=self.low_qps_cores.split(","))
            except:
                print(f"ERROR: Trying to set the low qps mode on container {self.current_job.value}")
                raise Exception("Error while setting the low qps mode on container {self.current_job.value}")

    def run(self):
        self.total = {"start": time()}
        for job in self.order:
            print(job)
            print(f"Starting the job {job}")
            self.container_start_end[job] = {"start": time()}
            self.current_job = job
            if self.mode == HIGH_QPS_MODE:
                self.run_or_unpause_container(job, cores=self.high_qps_cores, num_threads=3)
            else:
                self.run_or_unpause_container(job, cores=self.low_qps_cores, num_threads=3)
            self.wait_container(job)
            self.container_start_end[job]["end"] = time()
            self.container_start_end[job]["duration"] = self.container_start_end[job]["end"] - self.container_start_end[job]["start"]
            print(f"Container {job} ended")
        self.total["end"] = time()
        self.total["duration"] = self.total["end"] - self.total["start"]
        print(f"All jobs ended. Total Duration: {self.total['duration']}")


scheduler = Scheduler()
scheduler.run()

