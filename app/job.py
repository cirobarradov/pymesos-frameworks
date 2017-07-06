import textwrap
from addict import Dict
from six import iteritems
import logging
import sys

logger = logging.getLogger(__name__)

class Job(object):

    def __init__(self, name, num, cpus=1.0, mem=1024.0,
                 gpus=0, cmd=None, start=0):
        self.name = name
        self.num = num
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.cmd = cmd
        self.start = start


class Task(object):
    def __init__(self, mesos_task_id, job_name, task_index,
                 cpus=1.0, mem=1024.0, gpus=0, cmd=None, volumes={}):
        self.mesos_task_id = str(mesos_task_id)
        self.job_name = job_name
        self.task_index = task_index

        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.cmd = cmd
        self.volumes = volumes
        self.offered = False
        self.initalized = False

    def __str__(self):
        return textwrap.dedent('''
        <Task
          mesos_task_id=%s
        >''' % (self.mesos_task_id))

    def to_task_info(self, offer,
                     image ,
                     cmd,
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

        if image is not None:
            ti.container.type = 'DOCKER'
            ti.container.docker.image = image
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
        ti.command.value = cmd
        ti.command.environment.variables = variables = []
        env = Dict()
        variables.append(env)
        env.name = 'PYTHONPATH'
        env.value = ':'.join(sys.path)
        return ti


    '''
                    task = Dict()
                task_id = str(uuid.uuid4())
                task.task_id.value = task_id
                task.agent_id.value = offer.agent_id.value
                task.name = 'task {}'.format(task_id)
                task.container.type = 'DOCKER'
                task.container.docker.image = self._task_imp
                task.container.docker.network = 'HOST'
                task.container.docker.force_pull_image = True

                task.resources = [
                    dict(name='cpus', type='SCALAR', scalar={'value': constants.TASK_CPU}),
                    dict(name='mem', type='SCALAR', scalar={'value': constants.TASK_MEM}),
                ]
                task.command.shell = True
                task.command.value = '/app/task.sh ' + self._redis_server + " " + "task.py" +" " + self._key'''
