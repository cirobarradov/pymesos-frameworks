from __future__ import print_function
import sys
import time
import socket
import signal
import getpass
import redis
import constants
import rhelper
from threading import Thread, Timer

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict
import logging
import math
from job import Job

logging.basicConfig(level=logging.DEBUG)
FOREVER = 0xFFFFFFFF

class MinimalScheduler(Scheduler):
    def __init__(self, master, max_tasks, connection, fwk_name, redis_server,jobs_def, volumes={},
                 forcePullImage=False):
        self._redis = connection
        self._master = master
        self._max_tasks = max_tasks
        self._helper = rhelper.Helper(connection,fwk_name)
        self._fwk_name = fwk_name
        self.accept_offers = True
        self._timers = {}

        self._redis_server = redis_server
        self.tasks = []
        self.job_finished = {}
        self._forcePullImage=forcePullImage
        self.task_spec=jobs_def
        for job in jobs_def:
            self.addJob(job)

    def addJob(self, job):
        if (job is not None):
            j=Job(job)
            self.job_finished[j.name] = j.num
            self.tasks.extend(j.tasks)

    def registered(self, driver, frameworkId, masterInfo):
        # set max tasks to framework registered
        logging.info("************registered     ")
        self._helper.register(frameworkId['value'], masterInfo)
        logging.info("<---")

    def reregistered(self, driver, masterInfo):
        logging.info("************re-registered  ")
        self._helper.reregister(masterInfo)
        self.reconcileTasksFromState(driver, self._helper.getTasks())
        logging.info("<---")

    '''
    Method that get all task from framework state and send them to be reconciled
    '''
    def reconcileTasksFromState(self,driver,tasks):
        logging.info("RECONCILE TASKS")
        if tasks is not None:
            #if there are tasks to reconcile, no offer will be acepted until finishing these tasks
            self._helper.reconcileUp(driver, tasks)


    def resourceOffers(self, driver, offers):
        logging.info(offers)
        filters = {'refuse_seconds': 5}

        self.addJob(self._helper.getPubSubJob())

        for offer in offers:
            #if all(task.offered for task in self.tasks):
            #    driver.suppressOffers()
            #    driver.declineOffer(offer.id, Dict(refuse_seconds=FOREVER))
            #    continue

            offered_cpus = offered_mem = 0.0
            offered_gpus = []
            offered_tasks = []
            gpu_resource_type = None

            for resource in offer.resources:
                if resource.name == 'cpus':
                    offered_cpus = resource.scalar.value
                elif resource.name == 'mem':
                    offered_mem = resource.scalar.value
                elif resource.name == 'gpus':
                    if resource.type == 'SET':
                        offered_gpus = resource.set.item
                    else:
                        offered_gpus = list(range(int(resource.scalar.value)))
                    #    offered_cpus = self.getResource(offer.resources, 'cpus')
                    #    offered_mem = self.getResource(offer.resources, 'mem')
                    #gpu_resource_type = resource.type

            for task in self.tasks:
                if task.offered:
                    continue

                if not (task.cpus <= offered_cpus and
                                task.mem <= offered_mem and
                                task.gpus <= len(offered_gpus)):
                    continue

                offered_cpus -= task.cpus
                offered_mem -= task.mem
                gpus = int(math.ceil(task.gpus))
                gpu_uuids = offered_gpus[:gpus]
                offered_gpus = offered_gpus[gpus:]
                task.offered = True
                #TODO parametrizar CMD
                #cmd = '/app/task.sh ' + self._redis_server + " " + "task.py" +" " + self._key
                ti=task.to_task_info(offer)
                offered_tasks.append(ti)
                logging.info(
                    "launch task name:" + task.job_name +"/" + task.mesos_task_id + " resources: " + \
                    ",".join(str(x) for x in ti.resources))

                self._helper.addTaskToState(self._helper.initUpdateValue(task.mesos_task_id))

                self._timers[task.mesos_task_id] = Timer(10.0, self.validateRunning, kwargs={'taskid': task.mesos_task_id, 'driver': driver})
                self._timers[task.mesos_task_id].start()

            driver.launchTasks(offer.id, offered_tasks,filters)


    def validateRunning(self, **kwargs):
        del self._timers[kwargs['taskid']]
        kwargs['driver'].reconcileTasks([dict(task_id={'value':kwargs['taskid']})])

    #def getResource(self, res, name):
    #    for r in res:
     #       if r.name == name:
    #            return r.scalar.value
     #   return 0.0

    def statusUpdate(self, driver, update):
        logging.debug('Status update TID %s %s',
                      update.task_id.value,
                      update.state)
        if update.task_id.value in self._timers.keys():
            self._timers[update.task_id.value].cancel()
            del self._timers[update.task_id.value]

        #self._helper.addTaskToState(update)
        if self._helper.isFinalState(update.state) :
            logging.info("terminal state for task: " + update.task_id.value)

            if update.state == 'TASK_FAILED':
                # print message if task fails
                logging.info(update.message)
            elif update.state == 'TASK_FINISHED':
                mesos_task_id = int(update.task_id.value)
                task = self.tasks[mesos_task_id]
                self.job_finished[task.job_name] -= 1

                if (self.job_finished[task.job_name] == 0):
                    logging.info(" ###############   " +task.job_name + " IS FINISHED #########################")

                self._helper.removeTaskFromState(update.task_id.value)

            logging.info(
                "tasks used = " + str(
                    self._helper.getNumberOfTasks()) + " of " + self._max_tasks)

            #logging.info(" CHECK RECONCILE STATUS UPDATE")
            #logging.info(self._helper.getReconcileStatus())
            #logging.info(self._helper.getNumberOfTasks())

            # reviveoffers if reconciled
            self._helper.reconcileDown(driver)
        else:
            self._helper.addTaskToState(update)


def main( master, max_tasks, redis_server, fwkName):
    connection = redis.StrictRedis(host=redis_server, port=6379, db=0)
    framework = Dict()
    framework.user = getpass.getuser()
    framework.name = fwkName
    framework.hostname = socket.gethostname()
    if connection.hexists(framework.name, constants.REDIS_FW_ID):
        logging.info("framework id already registered in redis")
        framework.id = dict(value=connection.get(":".join([framework.name, constants.REDIS_FW_ID])))
    cmd='/app/task.sh ' + redis_server + " " + "task.py" +" " + fwkName
    #TODO parametrizar jobs
    jobs_def = [
        {
            "name": "LinealRegressionAverage",
            "image":"cirobarradov/executor-app",
            "cmd" : cmd,
            "num": 3
        },
        {
            "name": "LinealRegression",
            "image": "cirobarradov/executor-app",
            "cmd": cmd,
            "num": 2
        }
    ]
    if connection.exists(":".join([framework.name, constants.REDIS_FW_ID])):
        logging.info("framework id already registered in redis")
        framework.id = dict(value=connection.get(":".join([framework.name, constants.REDIS_FW_ID])))

    driver = MesosSchedulerDriver(
        MinimalScheduler(master, max_tasks, connection, fwkName, redis_server, jobs_def),
        framework,
        master,
        use_addict=True,
    )

    def signal_handler(signal, frame):
        logging.info("Closing redis connection, cleaning scheduler data and stopping MesosSchdulerDriver")
        logging.info("Stop driver")
        driver.stop()

    def run_driver_thread():
        driver.run()

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    signal.signal(signal.SIGTERM, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)

    logging.info("Disconnect from redis")
    keys = connection.scan(match=":".join([framework.name, '*']))[1]
    logging.info(keys)
    entries = connection.delete(keys)
    logging.info(entries)
    connection = None


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: {} <master> <max_tasks> <redis_server> <fwkName>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
