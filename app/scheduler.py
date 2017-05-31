#!/usr/bin/env python2.7
from __future__ import print_function

import sys
import uuid
import time
import socket
import signal
import getpass
from threading import Thread
from os.path import abspath, join, dirname
import os
import redis

from pymesos import MesosSchedulerDriver, Scheduler, encode_data
from addict import Dict

TASK_CPU = 1
TASK_MEM = 32
EXECUTOR_CPUS = 1
EXECUTOR_MEM = 32

#class MinimalMesosSchedulerDriver(MesosSchedulerDriver):
#    def launchTasks(self, offerIds, tasks, filters=None):
#        logging.info("************LAUNCH TASKS ") 
#        logging.info(tasks)
#        logging.info("************LAUNCH TASKS") 
#        MesosSchedulerDriver.launchTasks(self,offerIds,tasks, filters)


class MinimalScheduler(Scheduler):

    def __init__(self,message):
        self._redis= redis.StrictRedis(host=os.getenv('REDIS_SERVER'), port=6379, db=0)      
        self._message = message
        
    def registered(self, driver, frameworkId, masterInfo):
        self._redis.set('foo', int(os.getenv('MAX_TASKS')))
        logging.info("************registered") 
        logging.info(frameworkId) 
        logging.info(masterInfo) 
        logging.info(self) 
        logging.info(driver) 
        logging.info("************registered") 
    def reregistered(self, driver, masterInfo):
        logging.info("************RE RE gistered") 
        logging.info(masterInfo) 
        logginf.info(self)
        logginf.info(driver)
        logging.info("************RE RE gistered") 
    def checkTask(framework):
        logging.info("redis-------------------------")
        logging.info(framework)
        self._redis.decr('foo')
        logging.info(self._redis.get('foo'))
        #queue????
        if self._redis.decr('foo')<0:
            raise TaskException('maximum number of tasks')

    def resourceOffers(self, driver, offers):
        filters = {'refuse_seconds': 5}        
        
        for offer in offers:
            try:
                checkTask(self.framework_id)
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
                task.command.value = '/app/task.sh '+self._message
                #task.command.arguments = [self._message]

                logging.info(task)            
                driver.launchTasks(offer.id, [task], filters)
            except TaskException:
                logging.info("TASK EXCEPTION")
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
    
#    driver = MinimalMesosSchedulerDriver(
#        MinimalScheduler(message),
#        framework,
#               os.getenv('MASTER'),
#        use_addict=True,
#    )

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
