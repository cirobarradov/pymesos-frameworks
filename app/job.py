import textwrap
from addict import Dict
from six import iteritems
import logging
import sys
logger = logging.getLogger(__name__)

class Job(object):
    def __init__(self, jobs):
        for job in jobs_def:
            j = Job(job)
        self.initFromDict(dict.get("name"),dict.get("num"),dict.get("tasks"),dict.get("cmd",None),dict.get("cpus",1.0),
        dict.get("mem",1024.0),dict.get("gpus",0),dict.get("start",0),dict.get("image", None),dict.get("volumes",{}))

    def __init__(self, dict):
        self.initFromDict(dict.get("name"),dict.get("num"),dict.get("tasks"),dict.get("cmd",None),dict.get("cpus",1.0),
        dict.get("mem",1024.0),dict.get("gpus",0),dict.get("start",0),dict.get("image", None),dict.get("volumes",{}))
#TODO granularizar job a nivel de tarea (recursos, imagen docker, proceso que lanza)
 #   def initFromDict(self, name, num, tasks, cmd=None, cpus=1.0, mem=1024.0,
 #                gpus=0, start=0, image=None, volumes={}):
    def initFromDict(self, name, num, tasks, cmd, cpus, mem,
                     gpus, start, image, volumes):
        self.name = name
        self.num = num
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.cmd = cmd
        self.start = start
        self.tasks=[]
        if tasks is None:
            for task_index in range(self.start, self.num):
                mesos_task_id = len(self.tasks)
                self.tasks.append(
                    Task(
                        mesos_task_id,
                        self.name,
                        task_index,
                        cmd=self.cmd,
                        cpus=self.cpus,
                        mem=self.mem,
                        gpus=self.gpus,
                        image=image,
                        volumes= volumes
                    )
                )
        else:
            self.num= len(tasks)
            for index, task in enumerate(tasks):
                self.tasks.append(
                    Task(
                        task.get('mesos_task_id',index),
                        self.name,
                        task.get('task_index', index),
                        task.get('cmd', self.cmd),
                        task.get('cpus', self.cpus),
                        task.get('mem', self.mem),
                        task.get('gpus', self.gpus),
                        task.get('image',image),
                        task.get('volumes', volumes),
                    )
                )


class Task(object):
    def __init__(self, mesos_task_id, job_name, task_index, cmd=None,
                 cpus=1.0, mem=1024.0, gpus=0, image=None, volumes={}):
        self.mesos_task_id = str(mesos_task_id)
        self.job_name = job_name
        self.task_index = task_index
        self.image=image
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.cmd = cmd
        self.volumes = volumes
        self.offered = False
        self.initalized = False

    # cmd = '/app/task.sh ' + self._redis_server + " " + "task.py" +" " + self._key
    def __str__(self):
        return textwrap.dedent('''
        <Task
          mesos_task_id=%s
        >''' % (self.mesos_task_id))

    def to_task_info(self, offer,
                     gpu_uuids=[],
                     gpu_resource_type=None,
                     force_pull_image=False):
        ti = Dict()
        ti.task_id.value = str(self.mesos_task_id)
        ti.agent_id.value = offer.agent_id.value
        ti.name = '/job:%s/task:%s' % (self.job_name, self.task_index)
        ti.resources = resources = []

        cpus = Dict()
        resources.append(cpus)
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = self.cpus

        mem = Dict()
        resources.append(mem)
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = self.mem

        if self.image is not None:
            ti.container.type = 'DOCKER'
            ti.container.docker.image = self.image
            ti.container.docker.force_pull_image = force_pull_image

            ti.container.docker.parameters = parameters = []
            p = Dict()
            p.key = 'memory-swap'
            p.value = '-1'
            parameters.append(p)

            ti.container.volumes = volumes = []

            for path in ['/etc/passwd', '/etc/group']:
                v = Dict()
                volumes.append(v)
                v.host_path = v.container_path = path
                v.mode = 'RO'

            for src, dst in iteritems(self.volumes):
                v = Dict()
                volumes.append(v)
                v.container_path = dst
                v.host_path = src
                v.mode = 'RW'

        if self.gpus and gpu_uuids and gpu_resource_type is not None:
            if gpu_resource_type == 'SET':
                gpus = Dict()
                resources.append(gpus)
                gpus.name = 'gpus'
                gpus.type = 'SET'
                gpus.set.item = gpu_uuids
            else:
                gpus = Dict()
                resources.append(gpus)
                gpus.name = 'gpus'
                gpus.type = 'SCALAR'
                gpus.scalar.value = len(gpu_uuids)

        ti.command.shell = True
        #cmd = [
        #    sys.executable, '-m', '%s.server' % __package__,
        #    str(self.mesos_task_id), master_addr
        #]
        ti.command.value = self.cmd
        ti.command.environment.variables = variables = []
        env = Dict()
        variables.append(env)
        env.name = 'PYTHONPATH'
        env.value = ':'.join(sys.path)
        return ti

if __name__ == '__main__':
    jobs_def = [
        {
            "name": "LinealRegressionAverage",
            "num": 3,
            "tasks": [{"mesos_task_id": 2, "job_name": "job", "task_index": 1,"cmd":"asdfasfasfdasdf"},
                      {"mesos_task_id": 0, "job_name": "job2", "task_index": 1, "cpus": 5,"image":"cirobarradov/fasdfasdf"}]
        },
        {
            "name": "LinealRegression",
            "num": 2,
            "cmd": '/app/task.sh task.py',
            "image": "cirobarradov/fasdfasdf"
        }
    ]


    for job in jobs_def:
        j=Job(job)
        for task in j.tasks:
            print("cmd " )
            print(task.cmd)
            print("cpu")
            print(task.cpus)
            print("image = ")
            print(task.image)
        print(j.name)
        print("#################")

