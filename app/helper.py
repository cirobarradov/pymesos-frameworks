
class Helper():

    def __init__(self,redis,fwk_name):
        self._redis = redis
        self._fwk_name= fwk_name

    def register(self, id, value):
        self._redis.hset(self._fwk_name, id, value)

    def getTasksSet(self,setName):
        tasks = self._redis.hget(self._fwk_name,setName)
        if tasks == None:
            res = set()
        else:
            res = eval(tasks)
        return res

    '''
    Method that checks if the scheduler can manage another task, checking if the number of tasks isn't greather than
     the maximum number of tasks parameter
    '''
    def checkTask(self, taskSetName,maxTasks):
        if self.getNumberOfTasks(taskSetName)>=int(maxTasks):
            print("Reached maximum number of tasks")
            raise Exception('maximum number of tasks')
        else:
            print(
                "number tasks used = " + self.getNumberOfTasks(taskSetName).__str__() + " of " + maxTasks)


    '''
    Method that adds a task to framework (key) state    
    '''
    def addTaskToState(self, taskSetName,task):
        aux = self.getTasksSet(taskSetName)
        tuple = (task.task_id.value, str(task))
        aux.add(tuple)
        self._redis.hset(self._fwk_name, taskSetName, aux)
    '''
    Method that removes a task from framework (key) state
    '''
    def removeTaskFromState(self,taskId,taskSetName):
        aux = self.getTasksSet(self._fwk_name, taskSetName)
        #aux = eval(self._redis.hget(key, taskSetName))
        d=dict(aux)
        tuple=(taskId,d[taskId])
        aux.remove(tuple)
        self._redis.hset(self._fwk_name, taskSetName, aux)

    '''
    Method that returns the number of tasks managed by one framework(key)
    '''
    def getNumberOfTasks(self,taskSetName):
        return len(self.getTasksSet( taskSetName))





