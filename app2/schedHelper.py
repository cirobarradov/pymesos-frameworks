from addict import Dict

import constants
from bean.status import StatusTask
from redisDao import RedisDao

import logging

logging.basicConfig(level=logging.DEBUG)

class SchedHelper():

    def __init__(self,redis_server,fwk_name):
        self._redisDao = RedisDao(redis_server)
        self.fwk_name= fwk_name
        #self._pubsub= redis.pubsub()
        #self._pubsub.subscribe("jobs")

    def register(self, fwkid, master_info):
        self._redisDao.set(":".join([self.fwk_name, constants.REDIS_FW_ID]), fwkid)
        self._redisDao.hmset(":".join([self.fwk_name, constants.REDIS_MASTER_INFO]), master_info)

    def reregister(self, master_info):
        self._redisDao.hmset(":".join([self.fwk_name, constants.REDIS_MASTER_INFO]), master_info)

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
        if (self.getNumberOfJobs() == 0 and self.getReconcileStatus()):
            self.setReconcileStatus(False)
            driver.reviveOffers()
    '''
    set reconcile flag as true or false
    '''
    def setReconcileStatus(self, reconcile):
        logging.info("set reconcile status")
        logging.info(reconcile)
        self._redisDao.hset(self.fwk_name, constants.REDIS_RECONCILE, reconcile)
        return reconcile

    def getReconcileStatus(self):
        logging.info("get reconcile status")
        rStatus = self._redisDao.hget(self.fwk_name, constants.REDIS_RECONCILE)
        return rStatus is not None and eval(rStatus)
    '''
    Method that get all task from framework state and send them to be reconciled
    '''
    def reconcileTasksFromState(self,driver,tasks):
        logging.info("RECONCILE TASKS")
        if tasks is not None:
            #if there are tasks to reconcile, no offer will be acepted until finishing these tasks
            self.reconcileUp(driver, tasks)


    '''
        filter all task ids depending on the task state
    '''
    def filterTasks(self,state):
        res=map(lambda x: x.replace(constants.STATE_KEY_TAG,constants.BLANK),
                filter(lambda x: (constants.STATE_KEY_TAG in x) and
                                 (self._redisDao.hget(self.fwk_name,x) == state),
                       self._redisDao.hkeys(self.fwk_name)))
        return res

    def convertTaskIdToSchedulerFormat(self, task):
        return eval(str(constants.PROTO_TASK_ID) % task)
    # return eval("{'task_id':{'value':\'%s\'}} " % task)

    def getTasksSet(self,setName):
        tasks = self._redisDao.hget(self.fwk_name,setName)
        if tasks == None:
            res = set()
        else:
            res = eval(tasks)
        return res
    '''
        get all task ids stored
    '''
    def getTasks(self):
        res=map(lambda x: x.replace(constants.STATE_KEY_TAG,constants.BLANK),
                filter(lambda x: constants.STATE_KEY_TAG in x, self._redisDao.hkeys(self.fwk_name)))
        return res



    '''
    Method that checks if the scheduler can manage another task, checking if the number of tasks isn't greather than
    the maximum number of jobs parameter
    Parameters
    ----------
    maxJobs (string) : maximum number of allowed jobs
    '''
    def checkTask(self,maxJobs):
        count = self.getNumberOfJobs()
        if count >= int(maxJobs):
            logging.info('maximum number of jobs')
            raise Exception('maximum number of jobs')
        else:
            logging.info("number tasks used = " + str(count) + " of " + maxJobs)


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
    def addTaskToState(self,updateTask, job_name):
        task=StatusTask.getTaskState(updateTask)
        self._redisDao.hmset(":".join([self.fwk_name, constants.REDIS_JOBS_SET, job_name,updateTask.task_id['value']]),
                          task)
    '''
    Method that removes a task from framework (key) state
    Parameters
    ----------
    taskId (string): identifier of the task
    '''
    def removeTaskFromState(self,task):
        self._redisDao.delete(":".join([self.fwk_name, constants.REDIS_JOBS_SET, task.job_name,task.mesos_task_id]))
        #self._redis.hdel(self._fwk_name, self.getContainerKey(taskId))
        #self._redis.hdel(self._fwk_name, self.getSourceKey(taskId))
        #self._redis.hdel(self._fwk_name, self.getStateKey(taskId))
        #self._redis.hdel(self._fwk_name, self.getAgentKey(taskId))


    '''
    Method that returns the number of tasks managed by one framework(key)
    '''
    def getNumberOfJobs(self):
        actualJobs = self._redisDao.scan(match=":".join([self.fwk_name, constants.REDIS_JOBS_SET, '*']))[1]
        d = dict(j.rsplit(':', 1) for j in actualJobs)
        return len(d.keys())

    def getFwkName(self):
        return self._redisDao.get(":".join([self.fwk_name, constants.REDIS_FW_ID]))

    def existsFwk(self):
        return self._redisDao.hexists(self.fwk_name, constants.REDIS_FW_ID)
    '''
        Publish/subscribe messages
    '''
    def getPubSubJob(self):
        m = self._pubsub.get_message()
        if m is not None and m.get("type")=="message":
            return eval(m.get("data"))
            #return json.loads(m.get("data"))
        else:
            return None