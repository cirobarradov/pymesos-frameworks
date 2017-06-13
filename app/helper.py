



class Helper():

    def __init__(self,redis):
        self._redis = redis

    def getTasksSet(self,key,setName):
        tasks = self._redis.hget(key,setName)
        if tasks == None:
            res = set()
        else:
            res = eval(tasks)
        return res

    '''
    Method that checks if the scheduler can manage another task, checking if the number of tasks isn't greather than
     the maximum number of tasks parameter
    '''
    def checkTask(self, key, taskSetName,maxTasks):
        if self.getNumberOfTasks(key,taskSetName)>=int(maxTasks):
            print("Reached maximum number of tasks")
            raise Exception('maximum number of tasks')
        else:
            print(
                "number tasks available = " + self.getNumberOfTasks(key,taskSetName).__str__() + " of " + maxTasks)


    '''
    Method that adds a task to framework (key) state    
    '''
    def addTaskToState(self, key, taskSetName,task):
        aux = self.getTasksSet(key,taskSetName)
        tuple = (task.task_id.value, str(task))
        aux.add(tuple)
        self._redis.hset(key, taskSetName, aux)
    '''
    Method that removes a task from framework (key) state
    '''
    def removeTaskFromState(self,key,taskId,taskSetName):
        aux = self.getTasksSet(key, taskSetName)
        #aux = eval(self._redis.hget(key, taskSetName))
        d=dict(aux)
        tuple=(taskId,d[taskId])
        aux.remove(tuple)
        self._redis.hset(key, taskSetName, aux)

    '''
    Method that returns the number of tasks managed by one framework(key)
    '''
    def getNumberOfTasks(self, key,taskSetName):
        return len(self.getTasksSet(key, taskSetName))
        #return len(eval(redis.hget(key, setName)))

