import docker
from enum import Enum
from scheduler_logger import Job, SchedulerLogger
from time import sleep, time
from threading import Thread, Lock
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
            Job.BLACKSCHOLES: 3,
            Job.CANNEAL: 3,
            Job.DEDUP: 1,
            Job.FERRET: 3,
            Job.FREQMINE: 3,
            Job.RADIX: 1,
            Job.VIPS: 3
        }

        self.container_data = {
            Job.BLACKSCHOLES: {},
            Job.CANNEAL: {},
            Job.DEDUP: {},
            Job.FERRET: {},
            Job.FREQMINE: {},
            Job.RADIX: {},
            Job.VIPS: {},
        }
        self.two_core_jobs = [Job.VIPS, Job.BLACKSCHOLES, Job.CANNEAL, Job.FERRET, Job.FREQMINE]
        self.one_core_jobs = [Job.DEDUP, Job.RADIX]
        self.two_core_job_idx = 0
        self.one_core_job_idx = 0
        self.stats = {}
        self.num_cores = 2

    def create_all_containers(self):
        for job in self.two_core_jobs:
            self.create_container(job, cores="2-3", num_threads=self.thread_mapping[job])
        for job in self.one_core_jobs:
            self.create_container(job, cores="1", num_threads=self.thread_mapping[job])

    def get_all_containers(self):
        try:
            return self.docker_client.containers.list(all=True)
        except docker.errors.APIError as e:
            print(f"ERROR: Returning all containers")
            print(e)
            return None

    def get_container_status(self, job:Job):
        try:
            container = self.docker_client.containers.get(job.value)
            container.reload()
            return container.status
        except docker.errors.NotFound as e:
            print(f"ERROR: Container not found")
            print(e)
            return None

    def get_container_command(self, job: Job, num_threads=1):
        if job.value == Job.RADIX.value:
            return f"./run -a run -S splash2x -p radix -i native -n {num_threads}"
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

    def get_image(self, job:Job):
        try:
            image = self.docker_client.images.get(f"anakli/cca:parsec_{job.value}")
            return image
        except docker.errors.ImageNotFound:
            print(f"Image Not Found for {job.value}")
            return None

    def remove_container(self, job:Job):
        container = self.get_container(job)
        if container is not None:
            container.remove()
        else:
            print(f"Container not found {job.value}")


    def create_container(self, job: Job, cores, num_threads=1, detach=True, auto_remove=False):
        container = self.get_container(job)
        if container is None:
            print(f"Container {job.value} does not exist. Creating container")
            image = self.get_image(job)
            if image is None:
                print(f"Image for container {job.value} not found. Pulling the image")
                try:
                    self.docker_client.images.pull(self.images[job])
                    print(f"SUCCESS: Image for {job.value} pulled.")
                except docker.errors.APIError as e:
                    print(f"ERROR: Trying to pull image for {job.value}")
                    raise e
                else:
                    container = self.docker_client.containers.create(cpuset_cpus=cores,
                                                name=job.value,
                                                detach=detach,
                                                auto_remove=auto_remove,
                                                image=self.images[job],
                                                command=self.get_container_command(job, num_threads=num_threads))
                    self.container_data[job]["cpu_set"] = cores
                    self.container_data[job]["initial_threads"] = num_threads
                    print(f"SUCCESS: Container for {job.value} created.")
                    container.reload()
                    return container
            else:
                container = self.docker_client.containers.create(cpuset_cpus=cores,
                                                name=job.value,
                                                detach=detach,
                                                auto_remove=auto_remove,
                                                image=self.images[job],
                                                command=self.get_container_command(job, num_threads=num_threads))
                self.container_data[job]["cpu_set"] = cores
                self.container_data[job]["initial_threads"] = num_threads
                print(f"SUCCESS: Container for {job.value} created.")
                container.reload()
                return container
        else:
            print(f"Container for {job.value} found.")
            self.container_data[job]["cpu_set"] = "0,1,2,3"
            self.container_data[job]["initial_threads"] = 1
            container.reload()
            return container

    def run_or_unpause_container(self, job:Job):
        container = self.get_container(job)
        if container is None:
            print(f"Container for {job.value} not found. You should create the container first.")
            return None
        if container.status == "running":
            print(f"Container {job.value} already running.")
            return container
        elif container.status == "paused":
            container.unpause()
            self.logger_client.job_unpause(job)
            print(f"Container {job.value} UNPAUSED.")
            return container
        elif container.status == "created":
            container.start()
            self.logger_client.job_start(job, initial_cores=self.container_data[job]["cpu_set"].split(","), initial_threads=self.container_data[job]["initial_threads"])
            print(f"Container {job.value} STARTED")
            return container
        else:
            print(f"Container status not known: {job.value} : {container.status}")
            return container

    def unpause_container(self, job:Job):
        container = self.get_container(job)
        if container is None:
            print(f"ERROR: UNPAUSE on container {job.value}. Container does not exist")
        else:
            container.reload()
            if container.status == "running":
                print(f"Container {job.value} already running.")
                return container
            elif container.status == "paused":
                try:
                    container.unpause()
                    self.logger_client.job_unpause(job)
                    print(f"Container {job.value} UNPAUSED")
                    return container
                except docker.errors.APIError as e:
                    print(f"ERROR: Unpausing container {job.value}")
                    print(e)
                    return None
            else:
                print(f"Container status unsuitable for unpausing. Status: {job.value}")
                return None

    def pause_container(self, job:Job):
        container = self.get_container(job)
        if container is None:
            print(f"ERROR on PAUSE container {job.value}: Container not found")
            return None
        else:
            container.reload()
            if container.status == "running" or container.status == "restarting":
                try:
                    container.pause()
                    self.logger_client.job_pause(job)
                    print(f"Container {job.value} PAUSED.")
                except docker.errors.APIError as e:
                    print(f"ERROR on PAUSE container {job.value}")
                    print(e)
            else:
                print(f"ERROR on PAUSE: Container {job.value} status not suitable. Status: {container.status}")

    def update_container(self, job:Job, cores):
        container = self.get_container(job)
        if container is None:
            print(f"ERROR on UPDATE container {job.value}. Container not found")
            return None
        else:
            container.reload()
            if container.status != "exited":
                try:
                    container.update(cpuset_cpus=cores)
                    self.container_data[job]["cpu_set"] = cores
                    self.logger_client.update_cores(job, cores=cores.split(","))
                    print(f"Container {job.value} UPDATED with {cores}")
                    return container
                except docker.errors.APIError as e:
                    print(f"ERROR on UPDATING container {job.value}")
                    print(e)
                    return None

    def set_two_core_mode(self):
        with self.mode_lock:
            if self.mode == TWO_CORE_MODE:
                return
            print(f"Switching to 2 cores for Containers.")
            self.mode = TWO_CORE_MODE
            if self.one_core_job_idx < len(self.one_core_jobs):
                container = self.get_container(self.one_core_jobs[self.one_core_job_idx])
                if container is None:
                    print(f"One core job Container not found during switching to 2 core mode. Current one core job index: {self.one_core_job_idx}")
                    return
                self.pause_container(self.one_core_jobs[self.one_core_job_idx])
            else:
                print(f"One core jobs are done.")
                if self.two_core_job_idx < len(self.two_core_jobs):
                    container = self.get_container(self.two_core_jobs[self.two_core_job_idx])
                    if container is None:
                        print(f"Two core job Container not found during switching to 2 core mode. Current two core job index: {self.two_core_job_idx}")
                        return
                    self.update_container(self.two_core_jobs[self.two_core_job_idx], cores=self.two_cores)
                else:
                    print(f"Two core jobs are also done")
                    return

    def set_three_core_mode(self):
        with self.mode_lock:
            if self.mode == THREE_CORE_MODE:
                return
            print(f"Switching to 3 cores for Containers.")
            self.mode = THREE_CORE_MODE
            if self.one_core_job_idx < len(self.one_core_jobs):
                container = self.get_container(self.one_core_jobs[self.one_core_job_idx])
                if container is None:
                    print(f"One core job Container not found during switching to 3 core mode. Current one core job index: {self.one_core_job_idx}")
                    return
                self.unpause_container(self.one_core_jobs[self.one_core_job_idx])
            else:
                print(f"One core jobs are done.")
                if self.two_core_job_idx < len(self.two_core_jobs):
                    container = self.get_container(self.two_core_jobs[self.two_core_job_idx])
                    if container is None:
                        print(f"Two core job container not found during switching to 3 core mode. Current two core job index: {self.two_core_job_idx}")
                        return
                    self.update_container(self.two_core_jobs[self.two_core_job_idx], cores=self.three_cores)
                else:
                    print(f"Two core jobs are also done")
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
        return self.one_core_job_idx >= len(self.one_core_jobs) and self.two_core_job_idx >= len(self.two_core_jobs)

    def one_core_jobs_done(self):
        return self.one_core_job_idx >= len(self.one_core_jobs)

    def two_core_jobs_done(self):
        return self.two_core_job_idx >= len(self.two_core_jobs)

    def check_queue1(self):
        if self.one_core_job_idx >= len(self.one_core_jobs):
            return None
        elif self.one_core_job_idx == len(self.one_core_jobs)-1:
            status = self.get_container_status(self.one_core_jobs[self.one_core_job_idx])
            if status != None and status == ContainerStatus.EXIT.value:
                self.logger_client.job_end(self.one_core_jobs[self.one_core_job_idx])
                self.one_core_job_idx += 1
                return None
            else:
                return self.one_core_jobs[self.one_core_job_idx]
        elif self.one_core_job_idx < len(self.one_core_jobs) - 1:
            status = self.get_container_status(self.one_core_jobs[self.one_core_job_idx])
            if status != None and status == ContainerStatus.EXIT.value:
                self.logger_client.job_end(self.one_core_jobs[self.one_core_job_idx])
                self.one_core_job_idx += 1
            return self.one_core_jobs[self.one_core_job_idx]

    def check_queue2(self):
        if self.two_core_job_idx >= len(self.two_core_jobs):
            return None
        elif self.two_core_job_idx == len(self.two_core_jobs)-1:
            status = self.get_container_status(self.two_core_jobs[self.two_core_job_idx])
            if status != None and status == ContainerStatus.EXIT.value:
                self.logger_client.job_end(self.two_core_jobs[self.two_core_job_idx])
                self.two_core_job_idx += 1
                return None
            else:
                return self.two_core_jobs[self.two_core_job_idx]
        elif self.two_core_job_idx < len(self.two_core_jobs) - 1:
            status = self.get_container_status(self.two_core_jobs[self.two_core_job_idx])
            if status != None and status == ContainerStatus.EXIT.value:
                self.logger_client.job_end(self.two_core_jobs[self.two_core_job_idx])
                self.two_core_job_idx += 1
            return self.two_core_jobs[self.two_core_job_idx]

    def get_container_cores(self, container:docker.models.containers.Container):
        return container.attrs["HostConfig"]["CpusetCpus"]


    def handle_cores(self, num_cores:int):
        if self.is_schedule_done():
            return
        print(f"Num cores is {num_cores}. Scheduler cores are {self.num_cores}")
        job1 = self.check_queue1()
        job2 = self.check_queue2()
        if job1 == None and job2 == None:
            return
        if num_cores == 2:
            status1 = None if job1 == None else self.get_container_status(job1)
            if job1 != None and status1 == ContainerStatus.RUN.value:
                self.pause_container(job1)
            if job2 != None:
                container2 = self.get_container(job2)
                if container2 == None:
                    self.create_container(job2, cores="2-3", num_threads=3)
                    self.run_or_unpause_container(job2)
                else:
                    if self.get_container_cores(container2) == "1-3":
                        self.update_container(job2, cores="2-3")
                        self.run_or_unpause_container(job2)
        elif num_cores == 3:
            if job1 != None:
                container1 = self.get_container(job1)
                if container1 == None:
                    self.create_container(job1, cores="1", num_threads=1)
                    self.run_or_unpause_container(job1)
                else:
                    if self.get_container_status(job1) == ContainerStatus.PAUSE.value:
                        self.unpause_container(job1)
            if job2 != None:
                container2 = self.get_container(job2)
                if container2 == None:
                    if job1 == None:
                        self.create_container(job2, cores="1-3", num_threads=3)
                        self.run_or_unpause_container(job2)
                    else:
                        self.create_container(job2, cores="2-3", num_threads=3)
                        self.run_or_unpause_container(job2)
                else:
                    if job1 == None:
                        if self.get_container_cores(container2) == "2-3":
                            self.update_container(job2, cores="1-3")
                            self.run_or_unpause_container(job2)
                    else:
                        if self.get_container_cores(container2) == "1-3":
                            self.update_container(job2, cores="2-3")
                            self.run_or_unpause_container(job2)
        self.num_cores = num_cores

    def run(self):
        self.create_all_containers()
        self.stats["total"] = {"start": time()}

        def one_core_jobs():
            while True:
                with self.mode_lock:
                    if self.one_core_jobs_done():
                        print(f"One core jobs are done.")
                        if not self.two_core_jobs_done():
                            if self.mode == TWO_CORE_MODE:
                                return
                            else:
                                self.update_container(self.two_core_jobs[self.two_core_job_idx], cores=self.three_cores)
                        return
                    else:
                        if self.two_core_jobs_done():
                            print(f"Two core jobs are done.")
                            if self.mode == TWO_CORE_MODE:
                                self.update_container(self.one_core_jobs[self.one_core_job_idx], cores=self.two_cores)
                            else:
                                self.update_container(self.one_core_jobs[self.one_core_job_idx], cores=self.three_cores)
                    one_core_job = self.one_core_jobs[self.one_core_job_idx]
                    print(f"Starting Container: {one_core_job.value}")
                    one_core_container = self.run_or_unpause_container(job=one_core_job)
                    self.container_data[one_core_job]["start"] = time()
                one_core_container.wait()
                self.container_data[one_core_job]["end"] = time()
                self.logger_client.job_end(one_core_job)
                duration = self.container_data[one_core_job]["end"] - self.container_data[one_core_job]["start"]
                print(f"Job {one_core_job.value} ended. Duration {duration}")
                self.one_core_job_idx += 1

        def two_core_jobs():
            while True:
                with self.mode_lock:
                    if self.two_core_jobs_done():
                        print(f"Two core jobs are done")
                        if not self.one_core_jobs_done():
                            if self.mode == TWO_CORE_MODE:
                                self.update_container(self.one_core_jobs[self.one_core_job_idx], cores=self.two_cores)
                            else:
                                self.update_container(self.one_core_jobs[self.one_core_job_idx], cores=self.three_cores)
                        return
                    else:
                        if self.one_core_jobs_done():
                            print(f"One core jobs are done.")
                        if self.mode == TWO_CORE_MODE:
                            continue
                        else:
                            self.update_container(self.two_core_jobs[self.two_core_job_idx], cores=self.three_cores)
                    two_core_job = self.two_core_jobs[self.two_core_job_idx]
                    print(f"Starting Container {two_core_job.value}")
                    two_core_container = self.run_or_unpause_container(job=two_core_job)
                    self.container_data[two_core_job]["start"] = time()
                two_core_container.wait()
                self.container_data[two_core_job]["end"] = time()
                self.logger_client.job_end(two_core_job)
                duration = self.container_data[two_core_job]["end"] - self.container_data[two_core_job]["start"]
                print(f"Job {two_core_job.value} ended. Duration {duration}")
                self.two_core_job_idx += 1
        thread = Thread(target=two_core_jobs, daemon=True)
        thread.start()
        one_core_jobs()
        thread.join()
        self.stats["total"]["end"] = time()
        self.stats["total"]["duration"] = self.stats["total"]["end"] - self.stats["total"]["start"]
        self.logger_client.end()
        print(self.stats)