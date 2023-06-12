import multiprocessing

from . import graph
from .resources import TaskStatus
from .pl_storage import PipelineStorage
from .zmqserver import ZMQServer, MessageBase

class Session:
  """
  A class representing a session for running molecule pipelines.

  This class provides methods for managing pipeline graphs and executing transforms.

  Attributes:
    in_sessions (tuple): A tuple of three `SeqSession` objects representing the three priority queues.
    in_port (int): The port number for the ZMQ server.
    store (PipelineStorage): An instance of `PipelineStorage` for storing pipeline graphs.
  """
  def __init__(self, in_sessions, in_port, store):
    """
    Initializes a `Session` object.

    Args:
      in_sessions (tuple): A tuple of three `SeqSession` objects representing the three priority queues.
      in_port (int): The port number for the ZMQ server.
      store (PipelineStorage): An instance of `PipelineStorage` for storing pipeline graphs.
    """
    self.session_p0, self.session_p1, self.session_p2 = in_sessions
    self.in_port = in_port
    self.store = store

  def getPQSession(self, queue):
    """
    Returns the `SeqSession` object corresponding to the given queue.

    Args:
      queue (str): A string representing the queue name.

    Returns:
      SeqSession: The `SeqSession` object corresponding to the given queue.
    """
    if queue == 'p0':
      return self.session_p0
    if queue == 'p1':
      return self.session_p1
    if queue == 'p2':
      return self.session_p2

  def listenForCommand(self):
    """
    Listens for incoming commands and processes them accordingly.
    """
    cmd = ZMQServer.getMessage(self.in_sock)
    cmd_str = cmd[MessageBase.COMMAND_KEY]
    cmd_param = cmd[MessageBase.COMMAND_PARAM_KEY]
    if cmd_str == MessageBase.COMMAND_PROCESS_GRAPH:
      p_hash, queue = cmd_param
      sg = self.store.loadPipeline(p_hash)
      session = self.getPQSession(queue)
      session.addSeqGraph(sg)
      ZMQServer.sendACK(self.in_sock, MessageBase.RESPONSE_GRAPH_PROCESSED)

    elif cmd_str == MessageBase.COMMAND_PROCESS_FINISH_TRANSFORM:
      for i in range(3):
        session = self.getPQSession('p'+str(i))
        t_hash = cmd_param[MessageBase.TRANSFORM_HASH]
        transform_precomputed_flag = cmd_param[MessageBase.TRANSFORM_PRECOMPUTED]
        task_update_run_time_flag = not transform_precomputed_flag
        session.markComplete(t_hash, task_update_run_time_flag=task_update_run_time_flag)
      ZMQServer.sendResponse(self.in_sock, MessageBase.RESPONSE_TRANSFORM_PROCESSED, cmd_param)

    elif cmd_str == MessageBase.COMMAND_PROCESS_FAILED_TRANSFORM:
      for i in range(3):
        session = self.getPQSession('p'+str(i))
        t_hash = cmd_param[MessageBase.TRANSFORM_HASH]
        transform_precomputed_flag = cmd_param[MessageBase.TRANSFORM_PRECOMPUTED]
        task_update_run_time_flag = not transform_precomputed_flag
        session.markFailed(t_hash, task_update_run_time_flag=task_update_run_time_flag)
      ZMQServer.sendResponse(self.in_sock, MessageBase.RESPONSE_TRANSFORM_PROCESSED, cmd_param)

    elif cmd_str == MessageBase.COMMAND_GET_NEXT_TRANSFORM:
      send_flag = False
      for i in range(3):
        session = self.getPQSession('p'+str(i))
        t_hash = session.nextTransform()
        if t_hash is not None:
          ZMQServer.sendResponse(self.in_sock, MessageBase.RESPONSE_NEXT_TRANSFORM, (t_hash, i))
          send_flag = True
          break
      if not send_flag:
        ZMQServer.sendResponse(self.in_sock, MessageBase.RESPONSE_NEXT_TRANSFORM, None)

    elif cmd_str == MessageBase.COMMAND_GET_CHECK_TRANSFORM:
      send_flag = False
      for i in range(3):
        session = self.getPQSession('p'+str(i))
        t_hash = session.checkTransform()
        if t_hash is not None:
          send_flag = True
          ZMQServer.sendResponse(self.in_sock, MessageBase.RESPONSE_CHECK_TRANSFORM, t_hash)
          break
      if not send_flag:
        ZMQServer.sendResponse(self.in_sock, MessageBase.RESPONSE_CHECK_TRANSFORM, None)

    elif cmd_str == MessageBase.COMMAND_ADD_TRANSFORM:
      t_hash = cmd_param
      for i in range(3):
        session = self.getPQSession('p' + str(i))
        session.appendZeroDeps(t_hash)
      ZMQServer.sendResponse(self.in_sock, MessageBase.RESPONSE_ADD_TRANSFORM, None)

    elif cmd_str == MessageBase.COMMAND_DELETE_GRAPH:
      for i in range(3):
        sg = self.store.loadPipeline(cmd_param)
        session = self.getPQSession('p'+str(i))
        session.removeSeqGraph(sg)
      ZMQServer.sendResponse(self.in_sock, MessageBase.RESPONSE_GRAPH_PROCESSED, None)

  def sessionLoop(self):
    """
    The main loop for the session process.

    This method listens for incoming commands on the ZMQ server socket and executes them.
    """
    self.in_sock = ZMQServer.getServerSock(self.in_port)
    while True:
      self.listenForCommand()

  def startSession(self):
    sess_process = multiprocessing.Process(target=self.sessionLoop, name='session')
    sess_process.start()

class SeqSession:
  """
  A class representing a sequence session for running molecule pipelines.

  This class provides methods for managing pipeline graphs and executing transforms.

  Attributes:
    in_sessions (tuple): A tuple of three `SeqSession` objects representing the three priority queues.
    in_port (int): The port number for the ZMQ server.
    store (PipelineStorage): An instance of `PipelineStorage` for storing pipeline graphs.
  """
  def __init__(self, db=None):
    """
    Initializes a new instance of the Session class.

    Args:
      db (object): An optional object representing a database connection.

    Attributes:
      dep_count (dict): A dictionary containing the number of dependencies for each task.
      ds_deps (dict): A dictionary containing the dependencies for each data source.
      tg_dict (dict): A dictionary containing information about each task.
      zero_deps (list): A list of tasks with zero dependencies.
      check_computed (list): A list of tasks that have already been checked for completion.
      in_hashes_complete (dict): A dictionary containing the completion status of each task.
      in_task_pipelines (dict): A dictionary containing the pipelines for each task.
      in_hashes_processing (set): A set of task hashes that are currently being processed.
      in_pipelines (dict): A dictionary containing the number of pipelines for each task.
      in_pipeline_graphs (dict): A dictionary containing the pipeline graphs for each task.
      db (object): An object representing a database connection.
    """
    self.dep_count = {}
    self.ds_deps = {}
    self.tg_dict = {}
    self.zero_deps = []
    self.check_computed = []
    self.in_hashes_complete = {}
    self.in_task_pipelines = {}
    self.in_hashes_processing = set()
    self.in_pipelines = {}
    self.in_pipeline_graphs = {}
    self.db = db

  def addSeqGraph(self, sg):
    """
    Adds a sequence graph to the session.

    Args:
      sg (dict): A dictionary representing the sequence graph to add.

    Returns:
      None
    """
    p_hash = PipelineStorage.computePipelineHash(sg)
    self.in_pipelines[p_hash] = len(sg['tg_dict'].keys())
    self.in_pipeline_graphs[p_hash] = graph.generateGraph(sg)
    for t_hash, ti in sg['tg_dict'].items():
      self.tg_dict[t_hash] = ti
      self.dep_count[t_hash] = sg['dep_count'][t_hash]
      if self.in_task_pipelines.get(t_hash, None) is None:
        self.in_task_pipelines[t_hash] = [p_hash]
      else:
        if p_hash not in self.in_task_pipelines[t_hash]:
          self.in_task_pipelines[t_hash].append(p_hash)
      self.db.addTaskToDb(t_hash, ti)

    for ds_hash, t_list in sg['ds_deps'].items():
      for t_hash in t_list:
        if ds_hash not in self.ds_deps:
          self.ds_deps[ds_hash] = []

        if t_hash not in self.ds_deps[ds_hash]:
          if ds_hash not in self.in_hashes_complete:
            self.ds_deps[ds_hash].append(t_hash)
          else:
            self.dep_count[t_hash] = self.dep_count[t_hash] - 1

    for t_hash in sg['tg_list']:
      self.appendZeroDeps(t_hash)

  def removeSeqGraph(self, sg):
    """
    Removes a sequence graph from the session.

    Args:
      sg (dict): A dictionary representing the sequence graph to remove.

    Returns:
      None
    """
    p_hash = PipelineStorage.computePipelineHash(sg)
    for t_hash, ti in sg['tg_dict'].items():
      if self.in_task_pipelines.get(t_hash, None) is not None:
        self.in_task_pipelines[t_hash].remove(p_hash)
      if self.in_task_pipelines.get(t_hash, None) is not None and len(self.in_task_pipelines[t_hash]) == 0:
        del self.in_task_pipelines[t_hash]
        if self.tg_dict.get(t_hash, None) is not None:
          if t_hash not in self.in_hashes_processing:
            del self.tg_dict[t_hash]
        if self.dep_count.get(t_hash, None) is not None:
          if self.dep_count[t_hash] > 0:
            del self.dep_count[t_hash]

        for ds_hash, t_list in sg['ds_deps'].items():
          if t_hash in t_list:
            if ds_hash in self.ds_deps:
              del self.ds_deps[ds_hash]

      self.db.updateTaskInDb(t_hash, TaskStatus.TERMINATED, update_run_time=False)
    self.db.updatePipelineInDb(p_hash, TaskStatus.TERMINATED)
    try:
      del self.in_pipelines[p_hash]
    except KeyError:
      pass

  def nextTransform(self):
    """
    Returns the hash of the next transform to be executed.

    If there are transforms with zero dependencies, the first one in the list is returned.
    If there are no transforms with zero dependencies, None is returned.

    Returns:
      str or None: The hash of the next transform to be executed, or None if there are no transforms with zero dependencies.
    """
    if len(self.zero_deps) > 0:
      t_hash = self.zero_deps.pop(0)
      self.in_hashes_processing.add(t_hash)
      return t_hash
    else:
      # TODO: pause while no jobs
      return None

  def checkTransform(self):
    """
    Returns the hash of the next transform to be checked for completion.

    If there are transforms that have been computed but not yet checked for completion, the first one in the list is returned.
    If there are no transforms that have been computed but not yet checked for completion, None is returned.

    Returns:
      str or None: The hash of the next transform to be checked for completion, or None if there are no transforms that have been computed but not yet checked for completion.
    """
    if len(self.check_computed) > 0:
      t_hash = self.check_computed.pop(0)
      return t_hash
    else:
      return None

  def markComplete(self, t_hash, task_update_run_time_flag=False):
    """
    Marks a task as completed and updates the status of any pipelines that depend on it.

    Args:
      t_hash (str): The hash of the completed task.
      task_update_run_time_flag (bool, optional): A flag indicating whether to update the task's run time. Defaults to False.

    Returns:
      bool: True if the task was successfully marked as completed, False otherwise.
    """
    in_pipelines = self.in_pipelines.copy()
    in_task_pipelines = self.in_task_pipelines.copy()
    try:
      ti = self.tg_dict[t_hash]
      if t_hash in self.zero_deps:
        self.zero_deps.remove(t_hash)
      for o_hash in ti['hashes']['outputs'].values():
        self.markDSAvailable(o_hash)
      self.db.updateTaskInDb(t_hash, TaskStatus.COMPLETED, update_run_time=task_update_run_time_flag)
      if self.in_task_pipelines.get(t_hash, None) is not None:
        for p_hash in self.in_task_pipelines.get(t_hash, []):
          if self.in_pipelines.get(p_hash, None) is not None:
            self.in_pipelines[p_hash] -= 1
            self.db.updatePipelineInDb(p_hash, TaskStatus.PROCESSING)
            if self.in_pipelines[p_hash] <= 0:
              self.db.updatePipelineInDb(p_hash, TaskStatus.COMPLETED)
        del self.in_task_pipelines[t_hash]
      if t_hash in self.in_hashes_processing:
        self.in_hashes_processing.remove(t_hash)
      return True
    except Exception as _:
      self.in_pipelines = in_pipelines
      self.in_task_pipelines = in_task_pipelines
      return False

  def markFailed(self, t_hash, task_update_run_time_flag=False):
    """
    Marks a task as failed and updates the status of any pipelines that depend on it.

    Args:
      t_hash (str): The hash of the failed task.
      task_update_run_time_flag (bool, optional): A flag indicating whether to update the task's run time. Defaults to False.

    Returns:
      bool: True if the task was successfully marked as failed, False otherwise.
    """
    try:
      if self.in_task_pipelines.get(t_hash, None) is not None:
        for p_hash in self.in_task_pipelines[t_hash]:
          self.db.updatePipelineInDb(p_hash, TaskStatus.TERMINATED)
          # try:
          #   del self.in_pipelines[p_hash]
          # except KeyError:
          #   pass
          for ft_hash in self.in_pipeline_graphs[p_hash].getForwardDepsList(t_hash):
            self.db.updateTaskInDb(ft_hash, TaskStatus.STALE, update_run_time=task_update_run_time_flag)
        # del self.in_task_pipelines[t_hash]
      return True
    except Exception as _:
      return False

  def markDSAvailable(self, o_hash):
    """
    Marks a data source as available and updates the dependency count for any tasks that depend on it.

    Args:
      o_hash (str): The hash of the available data source.

    Returns:
      None
    """
    if o_hash in self.ds_deps.keys():
      for t_hash in self.ds_deps[o_hash]:
        self.dep_count[t_hash] = self.dep_count[t_hash] - 1
        self.appendZeroDeps(t_hash)
      self.ds_deps[o_hash] = []

  def appendZeroDeps(self, t_hash):
    """
    Adds a task hash to the list of tasks with zero dependencies.

    If a task has no dependencies, it is added to the `zero_deps` list. The `zero_deps` list is used to keep track of tasks that are ready to be executed.

    Args:
      t_hash (str): The hash of the task to be added to the `zero_deps` list.

    Returns:
      None
    """
    if self.dep_count.get(t_hash, -1) == 0:
      # All hashes are added to both zero_deps and check_computed.
      # in server, first a task is popped from check_computed and checked if it is already computed.
      # If yes, the it is marked complete and also popped from zero_deps.
      # Only the task still present in zero_deps are sent for execution.
      self.zero_deps.append(t_hash)
      self.check_computed.append(t_hash)
