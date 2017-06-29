from addict import Dict

import constants
from redis.connection import ConnectionError

import logging

logging.basicConfig(level=logging.DEBUG)

class Helper():

    def __init__(self,redis,fwk_name):
        self._redis = redis
        self._fwk_name= fwk_name

    def register(self, value):
        try:
            self._redis.hset(self._fwk_name, constants.REDIS_FW_ID, value)
        except ConnectionError:
            logging.info("ERROR exception error register")
    '''
        Check is some task is final
    '''
    def isFinalState(self,state):
        return  (state == constants.TASK_FINISHED) \
                or (state == constants.TASK_FAILED) \
                or (state == constants.TASK_KILLED) \
                or (state == constants.TASK_LOST) \
                or (state == constants.TASK_ERROR)


    '''
        Check is some task is final
    '''
    def checkReconciliation(self,state):
        return  (state == constants.TASK_LOST)
    '''
    Method than launches task reconciliation with a given set of tasks
    '''
    def reconcileUp(self,driver,tasks):
        logging.info("RECONCILE UP")
        self.setReconcileStatus(True)
        driver.suppressOffers()
        driver.reconcileTasks(
            map(lambda task: self.convertTaskIdToSchedulerFormat(task),
                tasks))

    def reconcileDown(self, driver):
        logging.info("RECONCILE DOWN")
        # reviveoffers if reconciled
        if (self.getNumberOfTasks() == 0 and self.getReconcileStatus()):
            self.setReconcileStatus(False)
            driver.reviveOffers()
    '''
    set reconcile flag as true or false
    '''
    def setReconcileStatus(self, reconcile):
        try:
            logging.info("set reconcile status")
            logging.info(reconcile)
            self._redis.hset(self._fwk_name, constants.REDIS_RECONCILE, reconcile)
        except ConnectionError:
            logging.info("ERROR exception error setReconcileStatus")
        return reconcile

    def getReconcileStatus(self):
        try:
            logging.info("get reconcile status")
            rStatus = self._redis.hget(self._fwk_name, constants.REDIS_RECONCILE)
            return rStatus is not None and eval(rStatus)
        except ConnectionError:
            logging.info("ERROR exception error getReconcileStatus")

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
            logging.info("ERROR exception error register")
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
            logging.info("ERROR exception error register")
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
        logging.info("CHECK TASK: " + str(self.getNumberOfTasks())+ " "+maxTasks)
        if self.getNumberOfTasks()>=int(maxTasks):
            logging.info("Reached maximum number of tasks")
            raise Exception('maximum number of tasks')
        else:
            logging.info(
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
            logging.info ("ERROR add task to state")
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
            logging.info ("ERROR remove Task From State")

    '''
    Method that returns the number of tasks managed by one framework(key)
    '''
    def getNumberOfTasks(self):
        try:
            return len(self._redis.hkeys(self._fwk_name))//4
        except ConnectionError:
            logging.info ("ERROR get Number Of Tasks")
