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

        self.is_done = False

        self.job_list = [ Job.RADIX, Job.FERRET, Job.FREQMINE, Job.CANNEAL, Job.VIPS, Job.BLACKSCHOLES, Job.DEDUP]
        #self.current_container = self.run_or_unpause_container(self.job_list[0], '2-3', 4)
        #self.logger_client.job_start(self.job_list[0], '2-3', 4)


        # We will parallelize between 3 cores
        self.core_3_jobs = [Job.FREQMINE, Job.BLACKSCHOLES]
        self.core_2_jobs = [Job.FERRET, Job.CANNEAL]
        self.core_1_jobs = [Job.DEDUP, Job.RADIX, Job.VIPS]

        self.current_jobs = [0,0,0]  # Job numbers in their respective queue

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
                print(f"SUCCESS: Container for {job.value} created.")
                container.reload()
                return container
        else:
            print(f"Container for {job.value} found.")
            container.reload()
            return container

    def run_or_unpause_container(self, job:Job, cores, num_threads=1, detach=True, auto_remove=False):
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
            self.logger_client.job_start(job, initial_cores=cores, initial_threads=num_threads)
            print(f"Container {job.value} STARTED")
            return container
        else:
            print(f"Container status not known: {job.value} : {container.status}")
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
        #return self.current_job >= len(self.job_list)
        return self.is_done
    
    def get_image(self, job:Job):
        try:
            image = self.docker_client.images.get(f"anakli/cca:parsec_{job.value}")
            return image
        except docker.errors.ImageNotFound:
            print(f"Image Not Found for {job.value}")
            return None

    def check_queue(self, queue_no, queue_jobs):
        # First we need to check if the core_3 queue is over.
        current_job_index = self.current_jobs[queue_no]
        num_jobs = len(queue_jobs)

        # Checking the current status of the container.
        if current_job_index >= num_jobs:
            return True, -1
        elif current_job_index == num_jobs - 1:
            status = self.get_container_status(queue_jobs[current_job_index].value)
            if status != None and status == ContainerStatus.EXIT.value:
                self.current_jobs[queue_no] += 1
                return True, -1
            else:
                return False, queue_jobs[self.current_jobs[queue_no]]
        elif current_job_index < num_jobs - 1:
            status = self.get_container_status(queue_jobs[current_job_index].value)
            # We can move to the next job in the queue
            if status != None and status == ContainerStatus.EXIT.value:
                self.current_jobs[queue_no] += 1
            return False, queue_jobs[self.current_jobs[queue_no]]

    # Handling the core usage
    def handle_core_usage_2(self, num_cores):
        if self.is_schedule_done():
            return

        is_available = [False, False, False] # Shows if any of the cores is actually available (done with the jobs).
        jobs_to_run = [None, None, None]

        # First we need to check if the core_3 queue is over.
        available, job = self.check_queue(2, self.core_3_jobs)
        is_available[2] = available
        jobs_to_run[2] = job if job != -1 else None

        # Then we need to check if the queue 2 is over.
        available, job = self.check_queue(1, self.core_2_jobs)
        is_available[1] = available
        jobs_to_run[1] = job if job != -1 else None

        # Then we need to check if the queue 1 is over.
        available, job = self.check_queue(0, self.core_1_jobs)
        is_available[0] = available
        jobs_to_run[0] = job if job != -1 else None

        # If num_cores are 2, it means that core_1 jobs wont be running.
        if num_cores == 2:
            print('We have 2 cores now')
            # Check if the core 1 job was running
            status = None if jobs_to_run[0] == None else self.get_container_status(jobs_to_run[0].value) 
            if status != None and status == ContainerStatus.RUN.value:
                self.pause_container(jobs_to_run[0])

            # So we need to check if any updates are necessary
            if is_available[1] and not is_available[2]:
                print('case 1')
                container = self.get_container(jobs_to_run[2])
                if container == None:
                    self.create_container(jobs_to_run[2], '2-3', self.thread_mapping[jobs_to_run[2]])
                    self.run_or_unpause_container(jobs_to_run[2], '2-3', self.thread_mapping[jobs_to_run[2]])
                else:
                    self.update_container(jobs_to_run[2], '2-3')
                    self.run_or_unpause_container(jobs_to_run[2], '2-3', self.thread_mapping[jobs_to_run[2]])
            elif not is_available[1] and is_available[2]:
                print('case 2')
                container = self.get_container(jobs_to_run[1])
                if container == None:
                    self.create_container(jobs_to_run[1], '2-3', self.thread_mapping[jobs_to_run[1]])
                    self.run_or_unpause_container(jobs_to_run[1], '2-3', self.thread_mapping[jobs_to_run[1]])
                else:
                    self.update_container(jobs_to_run[1], '2-3')
                    self.run_or_unpause_container(jobs_to_run[1], '2-3', self.thread_mapping[jobs_to_run[1]])
            elif not is_available[1] and not is_available[2]: 
                print('case 3')
                container = self.get_container(jobs_to_run[2])
                if container == None:
                    self.create_container(jobs_to_run[2], '3', self.thread_mapping[jobs_to_run[2]])
                self.run_or_unpause_container(jobs_to_run[2], '3', self.thread_mapping[jobs_to_run[2]])

                container = self.get_container(jobs_to_run[1])
                if container == None:
                    self.create_container(jobs_to_run[1], '2', self.thread_mapping[jobs_to_run[1]])
                self.run_or_unpause_container(jobs_to_run[1], '2', self.thread_mapping[jobs_to_run[1]])
            elif is_available[1] and is_available[2]:
                print('case 4')
                # If both of them are available, then we look at the low priority job
                if not is_available[0]:
                    container = self.get_container(jobs_to_run[0])
                    if container == None:
                        self.create_container(jobs_to_run[0], '2-3', self.thread_mapping[jobs_to_run[0]])
                        self.run_or_unpause_container(jobs_to_run[0], '2-3', self.thread_mapping[jobs_to_run[0]])
                    else:
                        self.update_container(jobs_to_run[0], '2-3')
                        self.run_or_unpause_container(jobs_to_run[0], '2-3', self.thread_mapping[jobs_to_run[0]])

        # If num_cores are 3, it means that all the queue jobs should work.
        elif num_cores == 3:
            def check_update_and_run(target_queue, other_queue_1, other_queue_2):
                # Other queue 1 should be smaller than 2.
                if other_queue_1 > other_queue_2:
                    temp = other_queue_2
                    other_queue_2 = other_queue_1
                    other_queue_1 = temp

                all_cores = '1-3'
                target_and_one = f'{target_queue+1},{other_queue_1+1}'
                target_and_second = f'{target_queue+1},{other_queue_2+1}'
                target = f'{target_queue+1}'

                if not is_available[target_queue]:
                    print('casee 1 ---')
                    container = self.get_container(jobs_to_run[target_queue])
                    if is_available[other_queue_1] and is_available[other_queue_2]:
                        print('case 1.1.')
                        if container == None:
                            self.create_container(jobs_to_run[target_queue], all_cores, self.thread_mapping[jobs_to_run[target_queue]])
                            self.run_or_unpause_container(jobs_to_run[target_queue], all_cores, self.thread_mapping[jobs_to_run[target_queue]])
                        else:
                            self.update_container(jobs_to_run[target_queue], all_cores)
                            self.run_or_unpause_container(jobs_to_run[target_queue], all_cores, self.thread_mapping[jobs_to_run[target_queue]])
                    elif is_available[other_queue_2] and not is_available[other_queue_1]:
                        print('case 1.2.')
                        if container == None:
                            self.create_container(jobs_to_run[target_queue], target_and_second, self.thread_mapping[jobs_to_run[target_queue]])
                            self.run_or_unpause_container(jobs_to_run[target_queue], target_and_second, self.thread_mapping[jobs_to_run[target_queue]])
                        else:
                            self.update_container(jobs_to_run[target_queue], target_and_second)
                            self.run_or_unpause_container(jobs_to_run[target_queue], target_and_second, self.thread_mapping[jobs_to_run[target_queue]])
                    elif not is_available[other_queue_2] and is_available[other_queue_1]:
                        print('case 1.3')
                        if container == None:
                            self.create_container(jobs_to_run[target_queue], target_and_one, self.thread_mapping[jobs_to_run[target_queue]])
                            self.run_or_unpause_container(jobs_to_run[target_queue], target_and_one, self.thread_mapping[jobs_to_run[target_queue]])
                        else:
                            self.update_container(jobs_to_run[target_queue], target_and_one)
                            self.run_or_unpause_container(jobs_to_run[target_queue], target_and_one, self.thread_mapping[jobs_to_run[target_queue]])
                    elif not is_available[other_queue_2] and not is_available[other_queue_1]:
                        print('case 1.4')
                        if container == None:
                            self.create_container(jobs_to_run[target_queue], target, self.thread_mapping[jobs_to_run[target_queue]])
                            self.run_or_unpause_container(jobs_to_run[target_queue], target, self.thread_mapping[jobs_to_run[target_queue]])
                        else:
                            self.update_container(jobs_to_run[target_queue], target)
                            self.run_or_unpause_container(jobs_to_run[target_queue], target, self.thread_mapping[jobs_to_run[target_queue]])

            print('we have 3 cores noewww')
            # Again we check if any updates are necessary.
            check_update_and_run(2,1,0)
            check_update_and_run(1,2,0)
            check_update_and_run(0,1,2)
        self.is_done = (is_available[0] and is_available[1] and is_available[2])
        



    
