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

    def getHealthKey(self,taskId):
        return taskId+constants.HEALTHY_KEY_TAG

    def getStatusKey(self, taskId):
        return taskId+constants.STATUS_KEY_TAG

    def getTaskState(self,taskId,healthy,status):
        task=Dict()
        healthKey=self.getHealthKey(taskId)
        statusKey=self.getStatusKey(taskId)

        task[healthKey] = healthy
        task[statusKey] = status
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
    taskId (string): identifier of the task
    healthy (string): healthy state of the task (true/false)
    status: current status of the task (running,lost,...)
    '''
    def addTaskToState(self,taskId,healthy,status):
        task=self.getTaskState(taskId,healthy,status)
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
        return len(self._redis.hkeys(self._fwk_name))//2

