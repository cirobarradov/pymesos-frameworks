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
import logging

logging.basicConfig(level=logging.INFO)


class BatchScheduler(Scheduler):
    def __init__(self, key, master, task_imp, max_tasks, connection, fwk_name, redis_server):
        logging.info("BATCH SCHEDULER---> INIT")
        self._redis = connection
        self._key = key
        self._master = master
        self._max_tasks = max_tasks
        self._task_imp = task_imp
        self._helper=rhelper.Helper(connection,fwk_name)
        self._fwk_name=fwk_name
        self._redis_server = redis_server
        logging.info("INIT <---BATCH SCHEDULER")

    def statusUpdate(self, driver, update):
        logging.info("BATCH SCHEDULER---> STATUS UPDATE")
        time.sleep(constants.DELAY_BATCH_TIME)
        if self._helper.checkReconciliation(update.state):
            # reviveoffers if reconciled
            self._helper.reconcileDown(driver)
        logging.info("STATUS UPDATE <---BATCH SCHEDULER")
