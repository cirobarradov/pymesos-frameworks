
from addict import Dict
import logging
import constants
logging.basicConfig(level=logging.DEBUG)

class StatusTask(object):
    def __init__(self, statusDict):
        self.task_id=statusDict.task_id.value
        self.state=statusDict.state
        self.executor_id=statusDict.executor_id.value
        self.uuid=statusDict.uuid
        self.timestamp=statusDict.timestamp
        self.reason=statusDict.reason
        self.container_status=statusDict.container_status.value
        self.source=statusDict.source
        self.agent_id=statusDict.agent_id.value
        self.message=statusDict.message

    def isFinalState(self):
        return (self.state in constants.TERMINAL_STATES)

    def isTaskFailed(self):
        return self.state==constants.TASK_FAILED

    def isTaskFinished(self):
        return self.state == constants.TASK_FINISHED

    '''
    return dict
    '''
    @staticmethod
    def initTask(taskId):
        status = Dict()
        status.executor_id = dict(value='')
        status.uuid = ''
        status.task_id = dict(value=taskId)
        status.container_status = dict()
        status.source = ''
        status.state = constants.TASK_STAGING
        status.agent_id = dict(value='')
        return status
    '''
    status dict
    '''
    @staticmethod
    def getTaskState(status):
        task = Dict()
        logging.info(status)
        task[constants.SOURCE_KEY_TAG] = status.source
        task[constants.STATE_KEY_TAG] = status.state
        task[constants.AGENT_KEY_TAG] = status.agent_id['value']
        return task

    def printStatus(self):
        logging.debug('Status update TID %s %s', self.task_id, self.state)
