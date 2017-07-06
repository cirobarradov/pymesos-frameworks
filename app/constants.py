'''
Constants.py
'''
#RESOURCES
TASK_CPU = 0.3
TASK_MEM = 250
TASK_GPU=0
EXECUTOR_CPUS = 1
EXECUTOR_MEM = 32
#HASH KEY SUFIX
BLANK = ''
CONTAINER_KEY_TAG = ':container'
SOURCE_KEY_TAG = ':source'
STATE_KEY_TAG = ':state'
AGENT_KEY_TAG = ':agent'

#CONSTANTS MINIMAL SCHEDULER
REDIS_TASKS_SET = "tasks"
REDIS_FW_ID='fwk_id'
REDIS_RECONCILE='reconcile'
REDIS_MASTER_INFO = 'master_info'

#CONSTANTS MESSAGES.PROTO
PROTO_TASK_ID={'task_id':{'value':'%s'}}
#PROTO_TASK_ID='task_id'
#PROTO_VALUE='value'
#TASKS STATES
TASK_STAGING = 'TASK_STAGING'
TASK_STARTING = 'TASK_STARTING'
TASK_RUNNING = 'TASK_RUNNING'
TASK_KILLING = 'TASK_KILLING'
#FINAL STATES
TASK_FINISHED = 'TASK_FINISHED'
TASK_FAILED = 'TASK_FAILED'
TASK_KILLED = 'TASK_KILLED'
TASK_LOST = 'TASK_LOST'
TASK_ERROR = 'TASK_ERROR'
TERMINAL_STATES = ["TASK_FINISHED","TASK_FAILED","TASK_KILLED","TASK_ERROR","TASK_LOST"]
#WAIT TIME
DELAY_BATCH_TIME=100