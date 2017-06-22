from addict import Dict

import constants
from redis.connection import ConnectionError
class Helper():

    def __init__(self,redis,fwk_name):
        self._redis = redis
        self._fwk_name= fwk_name

    def register(self, value):
        try:
            self._redis.hset(self._fwk_name, constants.REDIS_FW_ID, value)
        except ConnectionError:
            print("ERROR exception error register")

    '''
    set reconcile flag as true or false
    '''
    def setReconcileStatus(self, reconcile):
        try:
            print("set reconcile status")
            print(reconcile)
            self._redis.hset(self._fwk_name, constants.REDIS_RECONCILE, reconcile)
        except ConnectionError:
            print("ERROR exception error setReconcileStatus")

    def getReconcileStatus(self):
        try:
            print("get reconcile status")
            print (self._redis.hget(self._fwk_name, constants.REDIS_RECONCILE))
            print (self._redis.hget(self._fwk_name, constants.REDIS_RECONCILE)==True)
            return (self._redis.hget(self._fwk_name, constants.REDIS_RECONCILE)==True)
        except ConnectionError:
            print("ERROR exception error setReconcileStatus")
    '''
        filter all task ids depending on the task state
    '''
    def filterTasks(self,state):
        try:
            res=map(lambda x: x.replace(constants.STATE_KEY_TAG,constants.BLANK),
                    filter(lambda x: (constants.STATE_KEY_TAG in x) and
                                     (self._redis.hget(self._fwk_name,x) == state),
                           self._redis.hkeys(self._fwk_name)))
        except ConnectionError:
            print("ERROR exception error register")
        return res

    def convertTaskIdToSchedulerFormat(self, task):
        return eval(str(constants.PROTO_TASK_ID) % task)
    # return eval("{'task_id':{'value':\'%s\'}} " % task)
    '''
        get all task ids stored
    '''
    def getTasks(self):
        try:
            res=map(lambda x: x.replace(constants.STATE_KEY_TAG,constants.BLANK),
                    filter(lambda x: constants.STATE_KEY_TAG in x, self._redis.hkeys(self._fwk_name)))
        except ConnectionError:
            print("ERROR exception error register")
        return res

    def initUpdateValue(self,taskId):
        update=Dict()
        update.task_id=Dict()
        update.task_id.value=taskId
        update.container_status=''
        update.source=''
        update.state='RUNNING'
        update.agent_id=''
        return update
    '''
    methods to generate update keys
    '''
    def getContainerKey(self,taskId):
        return taskId+constants.CONTAINER_KEY_TAG
    def getSourceKey(self,taskId):
        return taskId+constants.SOURCE_KEY_TAG
    def getStateKey(self,taskId):
        return taskId+constants.STATE_KEY_TAG
    def getAgentKey(self,taskId):
        return taskId+constants.AGENT_KEY_TAG

    def getTaskState(self,update):
        task=Dict()
        #generate keys
        containerKey=self.getContainerKey(update.task_id.value)
        sourceKey=self.getSourceKey(update.task_id.value)
        stateKey=self.getStateKey(update.task_id.value)
        agentKey=self.getAgentKey(update.task_id.value)
        task[containerKey] = update.container_status
        task[sourceKey] = update.source
        task[stateKey] = update.state
        task[agentKey] = update.agent_id
        return task

    '''
    Method that checks if the scheduler can manage another task, checking if the number of tasks isn't greather than
    the maximum number of tasks parameter
    Parameters
    ----------
    maxTasks (string) : maximum number of allowed tasks
    '''
    def checkTask(self,maxTasks):
        print("CHECK TASK: " + str(self.getNumberOfTasks())+ " "+maxTasks)
        if self.getNumberOfTasks()>=int(maxTasks):
            print("Reached maximum number of tasks")
            raise Exception('maximum number of tasks')
        else:
            print(
                "number tasks used = " + self.getNumberOfTasks().__str__() + " of " + maxTasks)


    '''
    Method that adds a task to framework (key) state 
    Parameters
    ----------
    updateTask(Dictionary): information about task that contains:
        taskId (string): identifier of the task
        container(string)
        source (string)
        status (string): current status of the task (running,lost,...)
        agent (string)
    '''
    def addTaskToState(self,updateTask):
        try:
            task=self.getTaskState(updateTask)
            self._redis.hmset(self._fwk_name, task)
        except ConnectionError:
            print ("ERROR add task to state")
    '''
    Method that removes a task from framework (key) state
    Parameters
    ----------
    taskId (string): identifier of the task
    '''
    def removeTaskFromState(self,taskId):
        try:
            self._redis.hdel(self._fwk_name, self.getContainerKey(taskId))
            self._redis.hdel(self._fwk_name, self.getSourceKey(taskId))
            self._redis.hdel(self._fwk_name, self.getStateKey(taskId))
            self._redis.hdel(self._fwk_name, self.getAgentKey(taskId))
        except ConnectionError:
            print ("ERROR remove Task From State")

    '''
    Method that returns the number of tasks managed by one framework(key)
    '''
    def getNumberOfTasks(self):
        try:
            return len(self._redis.hkeys(self._fwk_name))//4
        except ConnectionError:
            print ("ERROR get Number Of Tasks")