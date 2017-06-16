from addict import Dict
import constants
class Helper():

    def __init__(self,redis,fwk_name):
        self._redis = redis
        self._fwk_name= fwk_name

    def register(self, value):
        self._redis.hset(self._fwk_name, constants.REDIS_FW_ID, value)

    def getTasksSet(self,setName):
        tasks = self._redis.hget(self._fwk_name,setName)
        if tasks == None:
            res = set()
        else:
            res = eval(tasks)
        return res

    def initUpdateValue(self,taskId):
        update=Dict()
        update.value=taskId
        update.container_status=''
        update.source=''
        update.state='STAGING'
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
        task=self.getTaskState(updateTask)
        self._redis.hmset(self._fwk_name, task)
    '''
    Method that removes a task from framework (key) state
    Parameters
    ----------
    taskId (string): identifier of the task
    '''
    def removeTaskFromState(self,taskId):
        self._redis.hdel(self._fwk_name,self.getHealthKey(taskId))
        self._redis.hdel(self._fwk_name, self.getStatusKey(taskId))

    '''
    Method that returns the number of tasks managed by one framework(key)
    '''
    def getNumberOfTasks(self):
        return len(self._redis.hkeys(self._fwk_name))//4

