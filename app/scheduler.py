from __future__ import print_function
import sys
import uuid
import time
import socket
import signal
import getpass
from threading import Thread
import os
import redis

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict

TASK_CPU = 0.2
TASK_MEM = 32
EXECUTOR_CPUS = 1
EXECUTOR_MEM = 32

REDIS_TASKS = "tasks"
REDIS_MAX_TASKS= "max_tasks"
REDIS_ID="fwk_id"
class MinimalScheduler(Scheduler):
    def __init__(self, message, conn):
        self._redis = conn
        self._message = message

    '''
    Method that get all task from framework (key) state and send them to be reconciled
    ''' 
    def reconcileTasksFromState(self,driver,key):
        logging.info("RECONCILE TASKS")
        tasks=[]
        redisTasks = self._redis.hget(key, REDIS_TASKS)
        if redisTasks is not None:
            logging.info("1")
            logging.info(type(redisTasks))
            logging.info("2")
            logging.info(redisTasks)
            logging.info("3")
            aux = eval(redisTasks)
            for elto in aux:
                tasks.append(eval(elto[1]))
            driver.reconcileTasks(tasks)
            
    '''
    Method that adds a task to framework (key) state
    '''
    def addTaskToState(self,key,task):
        logging.info("ADD TASK TO REDIS")
        aux = eval(self._redis.hget(key, REDIS_TASKS))
        tuple=(task.task_id.value,str(task))
        aux.add(tuple)
        self._redis.hset(key, REDIS_TASKS, aux)
    '''
    Method that removes a task from framework (key) state
    '''
    def removeTaskFromState(self,key,taskId):
        logging.info("REMOVE TASK FROM REDIS")
        aux = eval(self._redis.hget(key, REDIS_TASKS))
        d=dict(aux)
        tuple=(taskId,d[taskId])
        aux.remove(tuple)
        self._redis.hset(key, REDIS_TASKS, aux)
    '''
    Method that checks if redis has scheduler registered 
    '''
    def saveOrUpdateState(self, driver, key, max_tasks, id):
        if self._redis.exists(key):

            logging.info("framework already registered in redis")
            self.reconcileTasksFromState(driver,key)
            #self._redis.hset(key, 'max_tasks', max_tasks)
            logging.info("****************** OLD ID" + self._redis.hget(key, REDIS_ID))
            self._redis.hset(key, REDIS_ID, id)

        else:
            logging.info("framework NOT registered in redis")
            self._redis.hset(key, REDIS_MAX_TASKS, max_tasks)
            self._redis.hset(key, REDIS_ID, id)
            self._redis.hset(key, REDIS_TASKS, set())

        logging.info("****************** NEW ID" + self._redis.hget(key, REDIS_ID))
        logging.info("****************** MAX TASKS:" + self._redis.hget(key, REDIS_MAX_TASKS))
        logging.info("****************** TASKS:" + self._redis.hget(key, REDIS_TASKS))

    def registered(self, driver, frameworkId, masterInfo):
        # set max tasks to framework registered
        logging.info("************registered     " + frameworkId['value'])
        self.saveOrUpdateState(driver, driver._framework['name'],
                               int(os.getenv('MAX_TASKS')),
                               frameworkId['value'])
        logging.info("<---")

    def reregistered(self, driver, masterInfo):
        logging.info("************re-registered  ")
        logging.info(masterInfo)
        # logging.info(self)
        logging.info(driver)
        logging.info("<---")

    def checkTask(self, frameworkName):

        if int(self._redis.hget(frameworkName, REDIS_MAX_TASKS)) <= 0:
            logging.info("Reached xmaximum number of tasks")
            raise Exception('maximum number of tasks')
        else:
            logging.info("number tasks available = " + self._redis.hget(frameworkName,
                                                                        REDIS_MAX_TASKS) + " of " + os.getenv("MAX_TASKS"))
            self._redis.hincrby(frameworkName, 'max_tasks', -1)

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}
        for offer in offers:
            try:
                self.checkTask(driver._framework['name'])
                cpus = self.getResource(offer.resources, 'cpus')
                mem = self.getResource(offer.resources, 'mem')
                if cpus < TASK_CPU or mem < TASK_MEM:
                    continue

                task = Dict()
                task_id = str(uuid.uuid4())
                task.task_id.value = task_id
                task.agent_id.value = offer.agent_id.value
                task.name = 'task {}'.format(task_id)
                task.container.type = 'DOCKER'
                task.container.docker.image = os.getenv('DOCKER_TASK')
                task.container.docker.network = 'HOST'
                task.container.docker.force_pull_image = True

                task.resources = [
                    dict(name='cpus', type='SCALAR', scalar={'value': TASK_CPU}),
                    dict(name='mem', type='SCALAR', scalar={'value': TASK_MEM}),
                ]
                task.command.shell = True
                task.command.value = '/app/task.sh ' + self._message
                logging.info(
                    "launch task name:" + task.name + " resources: " + ",".join(str(x) for x in task.resources))
                #add task to the redis metadata
                self.addTaskToState(driver._framework['name'],task)
                driver.launchTasks(offer.id, [task], filters)
            except Exception:
                # traceback.print_exc()
                pass

    def getResource(self, res, name):
        for r in res:
            if r.name == name:
                return r.scalar.value
        return 0.0

    def statusUpdate(self, driver, update):
        logging.debug('Status update TID %s %s',
                      update.task_id.value,
                      update.state)
        if update.state == "TASK_FINISHED":
            logging.info("take another task for framework" + driver.framework_id + " " + driver._framework['name'])
            self._redis.hincrby(driver._framework['name'], REDIS_MAX_TASKS, 1)
            # remove task from the redis metadata
            self.removeTaskFromState(driver._framework['name'], update.task_id.value)
            logging.info("tasks availables = " + self._redis.hget(driver._framework['name'],
                                                                  REDIS_MAX_TASKS) + " of " + os.getenv("MAX_TASKS"))


def main(message):
    connection = redis.StrictRedis(host=os.getenv('REDIS_SERVER'), port=6379, db=0)
    framework = Dict()
    framework.user = getpass.getuser()
    framework.name = "MinimalFramework"
    framework.hostname = socket.gethostname()

    driver = MesosSchedulerDriver(
        MinimalScheduler(message, connection),
        framework,
        os.getenv('MASTER'),
        use_addict=True,
    )

    def signal_handler(signal, frame):
        logging.info("Stopping Driver and closing redis connection")
        driver.stop()

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print('master: {}'.format(os.getenv('MASTER')))
    print('Scheduler running, Ctrl+C to quit.')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 2:
        print("Usage: {} <message>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])
