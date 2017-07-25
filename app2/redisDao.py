from addict import Dict

import redis
from redis.connection import ConnectionError

import logging

logging.basicConfig(level=logging.DEBUG)

class RedisDao():

    def __init__(self,redis_server):
        self._redis = redis.StrictRedis(host=redis_server)
        self._server = redis_server

#BASIC OPERATIONS
    '''
        Set the value at key ``name`` to ``value``
    '''
    def set(self, name, value):
        try:
            self._redis.set(name, value)
        #except ConnectionError:
        #    logging.info("ERROR exception error register")
        except ConnectionError, e:
            logging.error('ERROR exception set method: ' + str(e))

    '''
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
    '''
    def hset(self, name, key, value):
        try:
            self._redis.hset(name, key, value)
        except ConnectionError, e:
            logging.error('ERROR exception hset method: ' + str(e))

    '''
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
    '''
    def hmset(self, name, mapping):
        try:
            self._redis.hmset(name, mapping)
        except ConnectionError, e:
            logging.error('ERROR exception hmset method: ' + str(e))

    '''
        Return the value of ``key`` within the hash ``name``
    '''
    def hget(self, name, key):
        try:
            return self._redis.hget(name, key)
        except ConnectionError, e:
            logging.error('ERROR exception hget method: ' + str(e))


    '''
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns        
    '''
    def scan(self, cursor=0, match=None, count=None):
        try:
            return self._redis.scan(cursor, match, count)
        except ConnectionError, e:
            logging.error('ERROR exception scan method: ' + str(e))

    '''
        Return the list of keys within hash ``name``
    '''
    def hkeys(self, name):
        try:
            return self._redis.hkeys(name)
        except ConnectionError, e:
            logging.error('ERROR exception hkeys method: ' + str(e))

    '''
        Returns a boolean indicating whether key ``name`` exists
    '''
    def exists(self, name):
        try:
            return self._redis.exists(name)
        except ConnectionError, e:
            logging.error('ERROR exception exists method: ' + str(e))

    '''
            Returns a boolean indicating if ``key`` exists within hash ``name``
    '''

    def hexists(self, name, key):
        try:
            return self._redis.hexists(name,key)
        except ConnectionError, e:
            logging.error('ERROR exception exists method: ' + str(e))

    '''
        Delete one or more keys specified by ``names``
    '''
    def delete(self, *names):
        return self._redis.delete(*names)

