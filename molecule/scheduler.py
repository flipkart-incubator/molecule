import multiprocessing

from .resources import TaskStatus, WorkerStatus
from .server import log
from .zmqserver import ZMQServer, MessageBase


class Scheduler:
  """
  The Scheduler class is responsible for managing the scheduling of tasks to workers.
  It listens for commands from the server and allocates tasks to available workers.
  """
  def __init__(self, in_port, store, db=None):
    """
    Initializes a new instance of the Scheduler class.

    Args:
      in_port (int): The port number to listen for commands on.
      store (object): The object that provides access to the data store.
      db (object, optional): The object that provides access to the database. Defaults to None.
    """
    self.in_port = in_port
    self.store = store
    self.db = db

  def listenForCommand(self):
    """
    Listens for a command from the server and processes it. The command can be one of the following:
    - COMMAND_START_TRANSFORM: Starts a transform task on a worker.
    - Any other command: Sends an ACK_ERROR response to the server.

    If the command is COMMAND_START_TRANSFORM, the method checks the heartbeat of the worker and sends an ACK_START_TRANSFORM
    response to the server if the heartbeat is successful. It then allocates the task to the worker and updates the task and worker
    status in the database. If the heartbeat is unsuccessful, it sends an ACK_ERROR response to the server and updates the worker
    status in the database to STALE.
    """
    cmd = ZMQServer.getMessage(self.in_sock)
    cmd_str = cmd[MessageBase.COMMAND_KEY]
    cmd_param = cmd[MessageBase.COMMAND_PARAM_KEY]
    t_hash = cmd_param[MessageBase.TRANSFORM_KEY]
    worker_id = cmd_param[MessageBase.WORKER_ID]
    wi = self.db.getWorkerFromDB(worker_id)
    worker_ip = wi['ip']
    worker_port = wi['port']

    if cmd_str == MessageBase.COMMAND_START_TRANSFORM:
      # check heartbeat
      worker_sock = ZMQServer.getClientSock(worker_ip, worker_port)
      response = ZMQServer.sendCommand(worker_sock, MessageBase.COMMAND_HEARTBEAT, None, MessageBase.ACK_HEARTBEAT)
      if response == MessageBase.ACK_HEARTBEAT:
        ZMQServer.sendResponse(self.in_sock, MessageBase.ACK_START_TRANSFORM, None)
        # start transform worker_id, ti
        log.info('Scheduling Job...')
        log.info([worker_id, t_hash])
        ZMQServer.sendCommand(worker_sock, MessageBase.COMMAND_ALLOCATE_WORKER, t_hash, MessageBase.ACK_ALLOCATE_WORKER)
        ZMQServer.closeSocket(worker_sock)
        self.db.updateTaskInDb(t_hash, TaskStatus.PROCESSING, worker_id, update_run_time=True)
        self.db.updateWorkerInDB(worker_id, WorkerStatus.BUSY, task_hash=t_hash)
      else:
        ZMQServer.closeSocket(worker_sock)
        log.warning('removing worker ' + worker_id)
        self.db.updateWorkerInDB(worker_id, WorkerStatus.STALE)
        ZMQServer.sendResponse(self.in_sock, MessageBase.ACK_ERROR, None)
    else:
      ZMQServer.sendResponse(self.in_sock, MessageBase.ACK_ERROR, None)

  def schedulerLoop(self):
    """
    The scheduler loop method listens for commands from the server and processes them indefinitely.
    """
    self.in_sock = ZMQServer.getServerSock(self.in_port)
    while True:
      self.listenForCommand()

  def startScheduler(self):
    """
    Starts the scheduler loop in a new process.
    """
    sch_process = multiprocessing.Process(target=self.schedulerLoop, name='scheduler')
    sch_process.start()
