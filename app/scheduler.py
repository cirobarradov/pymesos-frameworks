#!/usr/bin/env python2.7
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

TASK_CPU = 1
TASK_MEM = 32
EXECUTOR_CPUS = 1
EXECUTOR_MEM = 32


class MinimalScheduler(Scheduler):
    def __init__(self, message):
        self._redis = redis.StrictRedis(host=os.getenv('REDIS_SERVER'), port=6379, db=0)
        self._message = message

    def registered(self, driver, frameworkId, masterInfo):
        #set max tasks to framework registered
        logging.info("************registered     " + frameworkId['value'])
        self._redis.set(frameworkId['value'], int(os.getenv('MAX_TASKS')))
        #logging.info(masterInfo)
        #logging.info(driver)
        logging.info("<---")

    def reregistered(self, driver, masterInfo):
        logging.info("************RE RE gistered")
        logging.info(masterInfo)
        #logging.info(self)
        logging.info(driver)
        logging.info("<---")

    def checkTask(self, frameworkID):

        if int(self._redis.get(frameworkID)) <= 0:
            logging.info("maximum number of tasks")
            raise Exception('maximum number of tasks')
        else:
            logging.info("number tasks available = "+self._redis.get(frameworkID) + " of " + os.getenv("MAX_TASKS") )
            self._redis.decr(frameworkID)
        #logging.info(framework)
        #logging.info(_redis.get('foo'))
        # queue????
        #self._redis.decr('foo')
        #if self._redis.get('foo') < 0:
         #   raise Exception('maximum number of tasks')

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}
        for offer in offers:
            try:
                self.checkTask(driver.framework_id)
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
                # task.command.arguments = [self._message]
                #logging.info(task)
                logging.info("launch task name:" + task.name + " resources: "+ ",".join(str(x) for x in task.resources))
                driver.launchTasks(offer.id, [task], filters)
            except Exception:
                #traceback.print_exc()
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
            logging.info("take another task for framework" + driver.framework_id)
            self._redis.incr(driver.framework_id)
            logging.info("tasks availables = " + self._redis.get(driver.framework_id)+ " of "+os.getenv("MAX_TASKS"))
            
    def _send(self, body, path='/api/v1/scheduler', method='POST', headers={}):
        logging.info(body)
        logging.info(json.dumps(body).encode('utf-8'))
        with self._lock:
            conn = self._get_conn()
            if conn is None:
                raise RuntimeError('Not connected yet')

            if body != '':
                data = json.dumps(body).encode('utf-8')
                headers['Content-Type'] = 'application/json'
            else:
                data = ''

            stream_id = self.stream_id
            if stream_id:
                headers['Mesos-Stream-Id'] = stream_id

            if self._basic_credential:
                headers['Authorization'] = self._basic_credential

            try:
                conn.request(method, path, body=data, headers=headers)
                resp = conn.getresponse()
            except Exception:
                self._close()
                raise

            if resp.status < 200 or resp.status >= 300:
                raise RuntimeError('Failed to send request %s: %s\n%s' % (
                    resp.status, resp.read(), data))

            result = resp.read()
            if not result:
                return {}

            try:
                return json.loads(result.decode('utf-8'))
            except Exception:
                return {}
    
def main(message):
    framework = Dict()
    framework.user = getpass.getuser()
    framework.name = "MinimalFramework"
    framework.hostname = socket.gethostname()

    driver = MesosSchedulerDriver(
        MinimalScheduler(message),
        framework,
        os.getenv('MASTER'),
        use_addict=True,
    )


    def signal_handler(signal, frame):
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
