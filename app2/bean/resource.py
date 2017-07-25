from allocationInfo import AllocationInfo
from range import Range
import constants
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