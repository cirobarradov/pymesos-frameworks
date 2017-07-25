
import logging
from url import Url
import math
from resource import Resource
import constants
logging.basicConfig(level=logging.DEBUG)

class ResourceOffer(object):
    def __init__(self, offer):
        self.url=Url(offer.url)
        self.hostname=str(offer.hostname)
        self.framework_id=str(offer.framework_id.value)
        self.allocation_info=offer.allocation_info
        self.agent_id=str(offer.agent_id.value)
        self.id=str(offer.id.value)
        self.resources = dict(map(lambda resource: (str(resource.name), Resource(resource)), offer.resources))

        self.offered_cpus = self.resources.get(constants.RESOURCE_NAME_CPU).scalar
        self.offered_mem = self.resources.get(constants.RESOURCE_NAME_MEM).scalar
        self.offered_gpus= []
        '''
         for resource in offer.resources:
                    if resource.name == 'cpus':
                        offered_cpus = resource.scalar.value
                    elif resource.name == 'mem':
                        offered_mem = resource.scalar.value
                    elif resource.name == 'gpus':
                        if resource.type == 'SET':
                            offered_gpus = resource.set.item
                        else:
                            offered_gpus = list(range(int(resource.scalar.value)))'''
        self.offered_tasks = []
        self.gpu_resource_type = None

    def suitTask(self,task):
        return not (task.cpus <= self.offered_cpus and
                        task.mem <= self.offered_mem and
                        task.gpus <= len(self.offered_gpus))

    def updateResources(self,task):
        self.offered_cpus -= task.cpus
        self.offered_mem -= task.mem
        gpus = int(math.ceil(task.gpus))
        #gpu_uuids = self.offered_gpus[:gpus]
        self.offered_gpus = self.offered_gpus[gpus:]
        ti = task.to_task_info(self)
        self.offered_tasks.append(ti)
        return ti