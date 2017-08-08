
from addict import Dict
import logging
import constants
logging.basicConfig(level=logging.DEBUG)

class StatusTask(object):
    def __init__(self, statusDict=Dict()):
        self.task_id=Dict()
        self.task_id['value']=statusDict.task_id['value']
        self.state=statusDict.state
        self.executor_id=Dict()
        self.executor_id['value']=statusDict.executor_id['value']
        self.uuid=statusDict.uuid
        self.timestamp=statusDict.timestamp
        self.reason=statusDict.reason
        self.container_status=Dict()
        self.container_status['value']=statusDict.container_status['value']
        self.source=statusDict.source
        self.agent_id=Dict()
        self.agent_id['value']=statusDict.agent_id['value']
        self.message=statusDict.message

    def isFinalState(self):
        return (self.state in constants.TERMINAL_STATES)

    def isTaskFailed(self):
        return self.state==constants.TASK_FAILED

    def isTaskFinished(self):
        return self.state == constants.TASK_FINISHED

    def getTaskId(self):
        return self.task_id['value']

    '''
    return dict
    '''
    @staticmethod
    def initTask(taskId, fwk_name):
        statusDict=Dict()
        statusDict.reason='task asked by framework'
        statusDict.source='framework'
        statusDict.stage=constants.TASK_STAGING
        statusDict.task_id = dict(value=taskId)
        statusDict.executor_id = dict(value=fwk_name)

        status= StatusTask(statusDict)

        return status

    # def initUpdateValue(self, taskId):
    #     status = Dict()
    #     status.executor_id = dict(value=self._fwk_name)
    #     status.uuid = ''
    #     status.task_id = dict(value=taskId)
    #     status.container_status = dict()
    #     status.source = 'framework'
    #     status.reason = 'task asked by framework'
    #     status.state = 'TASK_STAGING'
    #
    #     status.agent_id = dict(value='')
    #     return status


    # def initTask(taskId):
    #     status = Dict()
    #     status.executor_id = dict(value='')
    #     status.uuid = ''
    #     status.task_id = dict(value=taskId)
    #     status.container_status = dict()
    #     status.source = ''
    #     status.state = constants.TASK_STAGING
    #     status.agent_id = dict(value='')
    #     return status
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
        logging.debug('Status update TID %s %s', self.task_id['value'], self.state)

#
# if __name__ == '__main__':
#     status = '{\'executor_id\': {}, \'uuid\': {}, \'task_id\': u\'2\', \'timestamp\': 1501063985.45066, \'state\': u\'TASK_STAGING\', \'container_status\': {}, \'source\': u\'SOURCE_MASTER\', \'reason\': u\'REASON_RECONCILIATION\', \'agent_id\': u\'5049f5bc-5630-4453-b14a-e39ed428f9b1-S4\', \'message\': u\'Reconciliation: Latest task state\'}'
#     status = eval(status)
#     sTask= StatusTask().initTask(2)
#     agent=sTask.agent_id['value']
#     print(agent)
#     StatusTask.getTaskState(sTask)