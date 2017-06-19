from __future__ import print_function
import sys
import uuid
import time
import socket
import signal
import getpass
from threading import Thread
import redis
import constants
import rhelper

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict



class MinimalScheduler(Scheduler):
    def __init__(self, message, master, task_imp, max_tasks, connection, fwk_name):
        self._redis = connection
        self._message = message
        self._master = master
        self._max_tasks = max_tasks
        self._task_imp = task_imp
        self._helper=rhelper.Helper(connection,fwk_name)

    def registered(self, driver, frameworkId, masterInfo):
        # set max tasks to framework registered
        logging.info("************registered     ")
        logging.info(frameworkId)
        self._helper.register( frameworkId['value'])
        logging.info("<---")

    def reregistered(self, driver, masterInfo):
        logging.info("************re-registered  ")
        logging.info(masterInfo)
        # logging.info(self)
        logging.info(driver)
        logging.info("<---")

    '''
    Method that get all task from framework (key) state and send them to be reconciled
    '''
    def reconcileTasksFromState(self,driver,key):
        print("RECONCILE TASKS")
        tasks=[]
        redisTasks = self._redis.hget(key, "tasks")
        if redisTasks is not None:
            print("1")
            print(type(redisTasks))
            print("2")
            print(redisTasks)
            print("3")
            print("RECONCILE TASKS")
            aux = eval(redisTasks)
            print(type(aux))
            for elto in aux:
                tasks.append(eval(elto[1]))
                print("4")
                print(tasks)
            driver.reconcileTasks(tasks)

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}
        for offer in offers:
            try:
                #checking if the framework can handle more tasks (looking the maximum number of allowed tasks)
                self._helper.checkTask(self._max_tasks)
                cpus = self.getResource(offer.resources, 'cpus')
                mem = self.getResource(offer.resources, 'mem')
                if cpus < constants.TASK_CPU or mem < constants.TASK_MEM:
                    continue
                task = Dict()
                task_id = str(uuid.uuid4())
                task.task_id.value = task_id
                task.agent_id.value = offer.agent_id.value
                task.name = 'task {}'.format(task_id)
                task.container.type = 'DOCKER'
                task.container.docker.image = self._task_imp
                task.container.docker.network = 'HOST'
                task.container.docker.force_pull_image = True

                task.resources = [
                    dict(name='cpus', type='SCALAR', scalar={'value': constants.TASK_CPU}),
                    dict(name='mem', type='SCALAR', scalar={'value': constants.TASK_MEM}),
                ]
                task.command.shell = True
                task.command.value = '/app/task.sh ' + self._message
                # task.command.arguments = [self._message]
                # logging.info(task)
                logging.info(
                    "launch task name:" + task.name + " resources: " + ",".join(str(x) for x in task.resources))
                logging.info("addtasktostate")
                logging.info(self._helper.initUpdateValue(task_id))
                metaDataTask=self._helper.initUpdateValue(task_id)
                self._helper.addTaskToState(metaDataTask)
                logging.info("launchtasks")
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
        self._helper.addTaskToState(update)
        if update.state == "TASK_FINISHED":
            logging.info("take another task for framework" + driver.framework_id)
            self._helper.removeTaskFromState(update.task_id.value)
            logging.info(
                "tasks used = " + str(
                    self._helper.getNumberOfTasks()) + " of " + self._max_tasks)


def main(message, master, task_imp, max_tasks, redis_server, fwkName):
    connection = redis.StrictRedis(host=redis_server, port=6379, db=0)

    framework = Dict()
    framework.user = getpass.getuser()
    framework.name = fwkName
    framework.hostname = socket.gethostname()
    if connection.hexists(framework.name, constants.REDIS_FW_ID):
        logging.info("framework id already registered in redis")
        framework.id = dict(value=connection.hget(framework.name, constants.REDIS_FW_ID))

    driver = MesosSchedulerDriver(
        MinimalScheduler(message, master, task_imp, max_tasks, connection, fwkName),
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
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)

    logging.info("Borramos redis")
    connection.delete(framework.name)

    logging.info("Disconnect from redis")
    connection.disconnect()
    connection = None


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 7:
        print("Usage: {} <message> <master> <task> <max_tasks> <redis_server> <fwkName>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
