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



class BatchScheduler(Scheduler):
    def __init__(self, message, master, task_imp, max_tasks, connection, fwk_name):
        print("BATCH SCHEDULER")
        self._redis = connection
        self._message = message
        self._master = master
        self._max_tasks = max_tasks
        self._task_imp = task_imp
        self._helper=rhelper.Helper(connection,fwk_name)
        self._fwk_name=fwk_name

    def statusUpdate(self, driver, update):
        print("HOLAHOLAHOLAHOLAHOLAHOLA")
        time.sleep(constants.DELAY_BATCH_TIME)
        print("ADIOSSSSSSSSSSSSSSSSSSSS")
        if self._helper.checkReconciliation(update.state):
            # reviveoffers if reconciled
            self._helper.reconcileDown(driver)
