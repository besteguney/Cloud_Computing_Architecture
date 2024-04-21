import psutil
import subprocess
import docker
from enum import Enum

# Parsec images (image_name, )
blackscholes = 'anakli/cca:parsec_blackscholes'


# Taken from https://docker-py.readthedocs.io/en/stable/containers.html
class ContainerStatus(Enum):
    RESTART='restarting'
    RUN='running'
    PAUSED='paused'
    EXITED='exited'

class DockerScheduler:
    def __init__(self) -> None:
        self.client = docker.from_env()
        self.jobs = [blackscholes]
        pass

    def get_all_containers(self):
        try:
            return self.client.containers.list(all=True)
        except docker.errors.APIError as e:
            print('fnjsdfhnj')
            print(e) 

    def get_containers_with_status(self, status:ContainerStatus):
        try:
            return self.client.containers.list(filters={'status':status.value})
        except docker.errors.APIError as e:
            print(e)

    def run_container(self, job_id, command, cpu_set, name, detach=True, remove=False):
        job = self.jobs[job_id]
        print(job)
        try:
            container = self.client.containers.run(job, command, cpuset_cpus=cpu_set, name=name, detach=detach, remove=remove)
            return container
            print('runnneddd')
        except docker.errors.ContainerError as e:
            print("jsdfjsdfjdsj")
            pass
        except docker.errors.ImageNotFound as e:
            print("No image")
            pass
        except docker.errors.APIError as e:
            print("apiii ")
    
    def update_container(self, container_id):
        pass



sh = DockerScheduler()
print(sh.get_all_containers())
print(sh.get_containers_with_status(ContainerStatus.EXITED))
print(sh.run_container(0, './run -a run -S parsec -p blackscholes -i native -n 2', "0", "parsec"))

