from __future__ import print_function

import getpass
import logging
import signal
import socket
import sys
import json
import time
from threading import Thread, Timer

from addict import Dict
from pymesos import MesosSchedulerDriver, Scheduler

from bean.job import Job
from bean.status import StatusTask
from bean.offer import ResourceOffer
from schedHelper import SchedHelper

logging.basicConfig(level=logging.DEBUG)

class TechlabScheduler(Scheduler):
    def __init__(self, max_jobs, helper, (jobs_def,cmd)):
        self._max_jobs = max_jobs
        self._helper = helper
        self.accept_offers = True
        self._timers = {}
        self._tasks = []
        self._job_finished = {}
        self._task_spec=jobs_def
        for job in jobs_def:
            self._addJob(job,cmd)

    #PRIVATE METHODS

    def _addJob(self, job, cmd=None):
        if (job is not None):
            j=Job(job,cmd)
            self._job_finished[j.name] = j.num
            self._tasks.extend(j.tasks)

    def _validateRunning(self, **kwargs):
        del self._timers[kwargs['taskid']]
        kwargs['driver'].reconcileTasks([dict(task_id={'value':kwargs['taskid']})])

    '''
    set metainformation on redis server when registered
    '''
    def registered(self, driver, frameworkId, masterInfo):
        logging.info("registered ")
        self._helper.register(frameworkId['value'], masterInfo)

    '''
        set metainformation on redis server when re-registered
    '''
    def reregistered(self, driver, masterInfo):
        logging.info("re-registered  ")
        self._helper.reregister(masterInfo)
        self._helper.reconcileTasksFromState(driver, self._helper.getTasks())


    '''
      Invoked when resources have been offered to this framework. A
      single offer will only contain resources from a single slave.
      Resources associated with an offer will not be re-offered to
      _this_ framework until either (a) this framework has rejected
      those resources (see SchedulerDriver::launchTasks) or (b) those
      resources have been rescinded (see Scheduler::offerRescinded).
      Note that resources may be concurrently offered to more than one
      framework at a time (depending on the allocator being used). In
      that case, the first framework to launch tasks using those
      resources will be able to use them while the other frameworks
      will have those resources rescinded (or if a framework has
      already launched tasks with those resources then those tasks will
      fail with a TASK_LOST status and a message saying as much).
    '''
    def resourceOffers(self, driver, offers):
        logging.info(offers)
        filters = {'refuse_seconds': 5}

        #TODO pubsub
        #self._addJob(self._helper.getPubSubJob())
        for offer in offers:
            try:
                offerResource=ResourceOffer(offer)
                #self._helper.checkTask(self._max_jobs)
                for task in self._tasks:
                    if task.offered:
                        continue
                    if offerResource.suitTask(task):
                        continue
                    ti=offerResource.updateResources(task)
                    task.offered = True

                    logging.info(
                        "launch task name:" + task.job_name +"/" + task.mesos_task_id + " resources: " + \
                        ",".join(str(x) for x in ti.resources))

                    self._helper.addTaskToState(StatusTask.initTask(task.mesos_task_id))

                    self._timers[task.mesos_task_id] = Timer(10.0, self._validateRunning,
                                                             kwargs={'taskid': task.mesos_task_id, 'driver': driver})
                    self._timers[task.mesos_task_id].start()

                driver.launchTasks(offer.id, offerResource.offered_tasks,filters)
            except Exception, e:
                logging.info(str(e))
                pass

    '''
     Invoked when the status of a task has changed (e.g., a slave is
     lost and so the task is lost, a task finishes and an executor
     sends a status update saying so, etc). If implicit
     acknowledgements are being used, then returning from this
     callback _acknowledges_ receipt of this status update! If for
     whatever reason the scheduler aborts during this callback (or
     the process exits) another status update will be delivered (note,
     however, that this is currently not true if the slave sending the
     status update is lost/fails during that time). If explicit
     acknowledgements are in use, the scheduler must acknowledge this
     status on the driver.
    '''
    def statusUpdate(self, driver, update):
        sTask=StatusTask(update)
        sTask.printStatus()

        if sTask.task_id in self._timers.keys():
            self._timers[sTask.task_id].cancel()
            del self._timers[sTask.task_id]

        if sTask.isFinalState():
            logging.info("terminal state for task: " + sTask.state)
            if sTask.isTaskFailed():
                logging.info(sTask.message)
            elif sTask.isTaskFinished():
                mesos_task_id = int(sTask.task_id)
                task = self._tasks[mesos_task_id]
                self._job_finished[task.job_name] -= 1

                if (self._job_finished[task.job_name] == 0):
                    logging.info(" ###############   " + task.job_name + " IS FINISHED #########################")
                self._helper.removeTaskFromState(sTask.task_id)
            logging.info(
                "tasks used = " + str(
                    self._helper.getNumberOfTasks()) + " of " + self._max_jobs)
            # reviveoffers if reconciled
            self._helper.reconcileDown(driver)
        else:
            self._helper.addTaskToState(vars(sTask))


def main( master, max_jobs, redis_server, fwkName):
    # connection = redis.StrictRedis(host=redis_server, port=6379, db=0)
    schedHelper = SchedHelper(redis_server, fwkName)

    framework = Dict()
    framework.user = getpass.getuser()
    framework.name = fwkName
    framework.hostname = socket.gethostname()

    if schedHelper.existsFwk():
        logging.info("framework id already registered in redis")
        framework.id = dict(value=schedHelper.getFwkName())

    cmd = '/app/task.sh ' + redis_server + " " + "task.py" + " " + fwkName
    jobs_def = json.loads(open('config/jobconfiguration.json').read())
    # TODO parametrizar jobs

    if schedHelper.existsFwk():
        logging.info("framework id already registered in redis")
        framework.id = dict(value=schedHelper.getFwkName())

    driver = MesosSchedulerDriver(
        TechlabScheduler(max_jobs, schedHelper, (jobs_def,cmd)),
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
    keys = schedHelper.scan(match=":".join([framework.name, '*']))[1]
    logging.info(keys)
    entries = schedHelper.delete(keys)
    logging.info(entries)
    connection = None

if __name__ == '__main__':
    '''
    Example 172.16.48.181 5 localhost TechlabScheduler
        master: Master DC/OS ip
        max_jobs: maximum number of parallel computing jobs
        redis_server: redis server ip
        fwkName: Name of the framework (metainfo)
    '''
    if len(sys.argv) != 5:
        print("Usage: {} <master> <max_jobs> <redis_server> <fwkName>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])