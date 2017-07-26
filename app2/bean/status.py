
from addict import Dict
import logging
import constants
logging.basicConfig(level=logging.DEBUG)

class StatusTask(object):
    def __init__(self, statusDict=Dict()):
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
        status=StatusTask()
        status.state=constants.TASK_STAGING
        status.task_id['value']=taskId
        return status
    '''
    status dict
    '''
    @staticmethod
    def getTaskState(status):
        task = Dict()
        logging.info(vars(status))
        task[constants.SOURCE_KEY_TAG] = status.source
        task[constants.STATE_KEY_TAG] = status.state
        task[constants.AGENT_KEY_TAG] = status.agent_id['value']
        return task

    def printStatus(self):
        logging.debug('Status update TID %s %s', self.task_id, self.state)


if __name__ == '__main__':
    status = '{\'executor_id\': {}, \'uuid\': {}, \'task_id\': u\'2\', \'timestamp\': 1501063985.45066, \'state\': u\'TASK_STAGING\', \'container_status\': {}, \'source\': u\'SOURCE_MASTER\', \'reason\': u\'REASON_RECONCILIATION\', \'agent_id\': u\'5049f5bc-5630-4453-b14a-e39ed428f9b1-S4\', \'message\': u\'Reconciliation: Latest task state\'}'
    status = eval(status)
    sTask= StatusTask().initTask(2)
    StatusTask.getTaskState(sTask)