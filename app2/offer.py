
import logging
import math


import constants
logging.basicConfig(level=logging.DEBUG)

class AllocationInfo(object):
    def __init__(self, allocation_info):
        self.role=str(allocation_info.role)

class Range(object):
    def __init__(self, range):
        self.begin=int(range.begin)
        self.end=int(range.end)

class Address(object):
    def __init__(self, address):
        self.ip=str(address.ip)
        self.hostname=str(address.hostname)
        self.port=str(address.port)

class Url(object):
    def __init__(self, url):
        self.path=str(url.path)
        self.scheme=str(url.scheme)
        self.address=Address(url.address)

class Resource(object):
    def __init__(self, resource):
        self.ranges=list(map(lambda range: Range(range),resource.ranges.range))
        self.role=str(resource.role)
        self.allocation_info=AllocationInfo(resource.allocation_info)
        self.type=str(resource.type)
        self.name=str(resource.name)
        if (self.isScalar()):
            self.scalar=resource.scalar.value

    def isScalar(self):
        return self.type==constants.RESOURCE_TYPE_SCALAR

    def isSet(self):
        return self.type==constants.RESOURCE_TYPE_SET

    def isRanges(self):
        return self.type==constants.RESOURCE_TYPE_RANGES

class ResourceOffer(object):
    def __init__(self, offer):
        self.url=Url(offer.url)
        self.hostname=str(offer.hostname)
        self.framework_id=str(offer.framework_id.value)
        self.allocation_info=AllocationInfo(offer.allocation_info)
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