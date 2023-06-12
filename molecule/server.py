import os
import traceback
import multiprocessing

from .task import TransformHashes
from .zmqserver import ZMQServer, MessageBase
from .resources import TaskStatus, WorkerStatus
from .queue_utils import QueueUtils, MultiProcessPriorityQueue
from . import pl_storage
from . import pl_logger as logger

log = logger.getLogger('server', enable_cloud_logging=False)


class Server:
  """
  The Server class represents the main server for the Molecule application. It manages the queues for tasks and transforms,
  communicates with the scheduler, session, and autoscaler, and processes completed tasks and transforms.
  """
  def __init__(self, in_port, scheduler_ip, scheduler_port, session_ip, session_port, autoscaler_ip, autoscaler_port, store, db=None):
    """
    Initializes the Server object with the specified parameters.

    Args:
      in_port (int): The port number on which the server will listen for incoming connections.
      scheduler_ip (str): The IP address of the scheduler.
      scheduler_port (int): The port number of the scheduler.
      session_ip (str): The IP address of the session.
      session_port (int): The port number of the session.
      autoscaler_ip (str): The IP address of the autoscaler.
      autoscaler_port (int): The port number of the autoscaler.
      store (object): The object representing the storage system.
      db (object, optional): The object representing the database. Defaults to None.
    """
    self.manager = multiprocessing.Manager()
    self.lock = self.manager.Lock()
    self.queue_utils_obj = QueueUtils(self.lock)
    self.PipelineQueue = self.manager.list()
    self.RemovePipelineQueue = self.manager.list()
    self.TransformScheduleQueue = MultiProcessPriorityQueue(self.manager)
    self.TransformProcessingQueue = self.manager.list()
    self.TransformCompletedQueue = self.manager.list()
    self.TransformRescheduleQueue = self.manager.list()
    self.ServiceReqQueue = self.manager.list()

    self.in_port = in_port
    self.scheduler_ip, self.scheduler_port = scheduler_ip, scheduler_port
    self.session_ip, self.session_port = session_ip, session_port
    self.autoscaler_ip, self.autoscaler_port = autoscaler_ip, autoscaler_port

    self.store = store
    self.db = db

  def processTCQueue(self):
    """
    Processes the Transform Completed Queue. For each completed transform, sends a message to the session to inform it
    that the transform is done or has failed.

    Returns:
      None
    """
    while len(self.TransformCompletedQueue) > 0:
      t_hash, status, transform_precomputed_flag = self.queue_utils_obj.popFromQueue(self.TransformCompletedQueue)
      t_dict = {
        MessageBase.TRANSFORM_HASH: t_hash,
        MessageBase.TRANSFORM_PRECOMPUTED: transform_precomputed_flag
      }
      # Just inform session that this task is done
      if status == 0:
        ZMQServer.sendRequest(self.session_sock, MessageBase.COMMAND_PROCESS_FINISH_TRANSFORM, t_dict)
      else:
        ZMQServer.sendRequest(self.session_sock, MessageBase.COMMAND_PROCESS_FAILED_TRANSFORM, t_dict)

  def processSRQueue(self):
    """
    Processes the Service Request Queue. Currently, this method does not perform any actions and simply passes over the queue.

    Returns:
      None
    """
    while len(self.ServiceReqQueue) > 0:
      pass

  def processPLQueue(self):
    """
    Processes the Pipeline Queue and Remove Pipeline Queue. Sends a command to the session to process the pipeline hash
    and specify the type of priority queue. Also sends a command to the session to remove the specified pipeline hash.

    Returns:
      None
    """
    while len(self.PipelineQueue) > 0:
      p_hash, queue = self.queue_utils_obj.popFromQueue(self.PipelineQueue)
      ZMQServer.sendRequest(self.session_sock, MessageBase.COMMAND_PROCESS_GRAPH, (p_hash, queue))
    while len(self.RemovePipelineQueue) > 0:
      p_hash = self.queue_utils_obj.popFromQueue(self.RemovePipelineQueue)
      ZMQServer.sendRequest(self.session_sock, MessageBase.COMMAND_DELETE_GRAPH, p_hash)

  def processTRQueue(self):
    """
    Processes the Transform Reschedule Queue. Sends a command to the session to add all the transforms in the queue for processing.

    Returns:
      None
    """
    while len(self.TransformRescheduleQueue) > 0:
      t_hash = self.queue_utils_obj.popFromQueue(self.TransformRescheduleQueue)
      ZMQServer.sendRequest(self.session_sock, MessageBase.COMMAND_ADD_TRANSFORM, t_hash)

  def checkComputed(self):
    """
    Checks if any transforms are already computed and marks them as finished in the session. If all the output hashes of a task
    are already computed, then the transform is skipped and marked as finished. If not, the transform is scheduled for processing.

    Returns:
      None
      """
    # For all transforms, see if it is already computed. If it's output hash is present.
    while True:
      # One by one session returns all the transform which needs to be checked.
      resp = ZMQServer.sendRequest(self.session_sock, MessageBase.COMMAND_GET_CHECK_TRANSFORM, None)
      cmd_str = resp[MessageBase.COMMAND_KEY]
      t_hash = resp[MessageBase.COMMAND_PARAM_KEY]
      if cmd_str == MessageBase.RESPONSE_CHECK_TRANSFORM and t_hash is not None:
        store = pl_storage.SpawnerStorage(self.store.store_loc, mode='gcs', db=self.db)
        computed_flag = True
        # Check if the hash is in currently running
        if self.queue_utils_obj.checkInQueue(self.TransformProcessingQueue, t_hash):
          computed_flag = False
          log.warning('Transform ' + t_hash + ' already running')
        # Check if all the output hashes of task are already computed. If not then move to next transform.
        # If all the output hashes of task are already computed, then computed_flag remains True and tranform is skipped
        else:
          thi = TransformHashes()
          t_dict = self.db.getTaskFromDb(t_hash)
          thi.populate(t_dict['hashes'])
          for _, output_hash in thi.outputs.items():
            if store.check(os.path.join(t_dict['data_dir'], output_hash)) is False:
              computed_flag = False
              break
        # If already computed then send a command to session to mark this transform as finished and resolve dependencies
        if computed_flag:
          log.info('Skipping ' + thi.transform['transform_class'] + ' ' + thi.transform['transform_type'] + ' ' + t_hash)
          t_dict = {
            MessageBase.TRANSFORM_HASH: t_hash,
            MessageBase.TRANSFORM_PRECOMPUTED: computed_flag
          }
          ZMQServer.sendRequest(self.session_sock, MessageBase.COMMAND_PROCESS_FINISH_TRANSFORM, t_dict)
      # If ti is None (none task is present) or the send request had some error, then break the loop.
      else:
        break
      
  def scheduleTransform(self, t_hash, worker_id):
    """
    Schedules a transform for processing by adding it to the TransformProcessingQueue and sending a command to the scheduler
    to start the transform. If there is an error, the transform is added to the TransformRescheduleQueue and removed from the
    TransformProcessingQueue.

    Args:
      t_hash (str): The hash of the transform to be scheduled.
      worker_id (str): The ID of the worker that will process the transform.

    Returns:
      None
    """
    task_dict = {
      MessageBase.TRANSFORM_KEY: t_hash,
      MessageBase.WORKER_ID: worker_id
    }
    # Send a command to schduler to start this transform
    self.queue_utils_obj.appendToQueue(self.TransformProcessingQueue, t_hash)
    resp = ZMQServer.sendRequest(self.scheduler_sock, MessageBase.COMMAND_START_TRANSFORM, task_dict)
    # If error, then add transform to TransformRescheduleQueue and remove from TransformProcessingQueue
    if resp[MessageBase.COMMAND_KEY] != MessageBase.ACK_START_TRANSFORM:
      log.warning("Rescheduling transform " + t_hash)
      self.queue_utils_obj.removeFromQueue(self.TransformProcessingQueue, t_hash)
      self.queue_utils_obj.appendToQueue(self.TransformRescheduleQueue, t_hash)
      
  def requestWorkers(self):
    while True:
      resp = ZMQServer.sendRequest(self.session_sock, MessageBase.COMMAND_GET_NEXT_TRANSFORM, None)
      cmd_str = resp[MessageBase.COMMAND_KEY]
      cmd_param = resp[MessageBase.COMMAND_PARAM_KEY]
      if cmd_str == MessageBase.RESPONSE_NEXT_TRANSFORM and cmd_param is not None:
        t_hash, queue = cmd_param
        # log.info("TRANSFORM BEING CHECKED: {} {}".format(t_hash, queue))
        # log.info("Transform present: {}".format(str(self.TransformScheduleQueue.checkInQueue(t_hash))))
        # log.info("Queue: {}".format(self.TransformScheduleQueue.print()))
        if (not self.queue_utils_obj.checkInQueue(self.TransformProcessingQueue, t_hash) and  
          not self.queue_utils_obj.checkInQueue(self.TransformCompletedQueue, t_hash) and 
          not self.queue_utils_obj.checkInQueue(self.TransformRescheduleQueue, t_hash) and 
          not self.TransformScheduleQueue.checkInQueue(t_hash)):
          
          req_worker_type = self.db.getTaskFromDb(t_hash)['worker_req']
          resp = ZMQServer.sendRequest(self.autoscaler_sock, MessageBase.REQUEST_NEW_FREE_WORKER, (req_worker_type, queue))
          if resp[MessageBase.COMMAND_KEY] == MessageBase.WORKER_ASSIGNED:
            worker_id = resp[MessageBase.COMMAND_PARAM_KEY]
            log.info("Task scheduled " + t_hash + " " + str(queue))
            self.scheduleTransform(t_hash, worker_id)
          else:
            self.TransformScheduleQueue.appendToQueue(t_hash, queue)
            # log.info("Queue After: {}".format(self.TransformScheduleQueue.print()))
        else:
          log.warning('Transform ' + t_hash + ' already scheduled ')
          pass
      else:
        break

  def processTransforms(self):
    """
    Process transforms in the TransformRescheduleQueue and TransformScheduleQueue. If a transform is in the
    TransformRescheduleQueue, then it is added to the TransformScheduleQueue and a request is sent to the autoscaler
    to assign a worker. If a transform is in the TransformScheduleQueue, then a request is sent to the autoscaler
    to assign a worker.
    """
    for queue in [0, 1, 2]:
      for _ in range(self.TransformScheduleQueue.getLen(queue)):
        t_hash = self.TransformScheduleQueue.popFromPriorityQueue(queue)
        req_worker_type = self.db.getTaskFromDb(t_hash)['worker_req']
        resp = ZMQServer.sendRequest(self.autoscaler_sock, MessageBase.REQUEST_FREE_WORKER, (req_worker_type, queue))
        if resp[MessageBase.COMMAND_KEY] == MessageBase.WORKER_ASSIGNED:
          worker_id = resp[MessageBase.COMMAND_PARAM_KEY]
          log.info("Task scheduled " + t_hash + " " + str(queue))
          self.scheduleTransform(t_hash, worker_id)
        elif resp[MessageBase.COMMAND_KEY] == MessageBase.WORKER_NOT_AVAILABLE:
          self.queue_utils_obj.appendToQueue(self.TransformRescheduleQueue, t_hash)
        else:
          self.TransformScheduleQueue.appendToQueue(t_hash, queue)

  def commandLoop(self):
    """
    The command loop listens for incoming commands from clients and workers and processes them accordingly.
    """
    self.command_sock = ZMQServer.getServerSock(self.in_port)
    global log
    while True:
      try:
        cmd = ZMQServer.getMessage(self.command_sock)
      except Exception as err:
        log.critical('Encountered yaml parse error, did you send something shady?')
        self.db.pushNotification(1, 'YAML Parse Error', 'Catch the culprit!')
        print(err)
        print(traceback.format_exc())
        continue

      ######## PIPELINE LEVEL COMMANDS ########
      # This command is recieved from Client. Add to proper queue here. Processing will be done in WorkLoop
      if cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_SUBMIT_GRAPH:
        try:
          data = cmd[MessageBase.COMMAND_PARAM_KEY]
          sg = data['task_dict']
          queue = data['queue']
          # if not data.get('enforce_ttl', False):
          #   raise Exception('TTL not enabled, pull the latest code!')
          ZMQServer.sendACK(self.command_sock, MessageBase.ACK_SUBMIT_GRAPH)
          self.store.savePipeline(sg)
          self.queue_utils_obj.appendToQueue(self.PipelineQueue, (self.store.computePipelineHash(sg), queue))
          if not self.db.addPipelineToDb(data):
            log.warning("Adding Pipeline to Database failed!")
        except Exception as _:
          ZMQServer.sendACK(self.command_sock, MessageBase.ACK_ERROR)

      # This command is recieved from Client. Add to proper queue here. Processing will be done in WorkLoop
      elif cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_DELETE_GRAPH:
        p_hash = cmd[MessageBase.COMMAND_PARAM_KEY]
        ZMQServer.sendACK(self.command_sock, MessageBase.ACK_DELETE_GRAPH)
        log.warning('Can\'t kill pipeline: ' + p_hash)
        # self.queue_utils_obj.appendToQueue(self.RemovePipelineQueue, p_hash)
        # if not self.db.updatePipelineInDb(p_hash, TaskStatus.TERMINATED):
          # log.warning("Pipeline updation to Database failed!")

      ######## TASK LEVEL COMMANDS ########
      elif cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_FINISH_TRANSFORM:
        cmd_param = cmd[MessageBase.COMMAND_PARAM_KEY]
        t_hash = cmd_param[MessageBase.TRANSFORM_HASH]
        transform_precomputed_flag = cmd_param[MessageBase.TRANSFORM_PRECOMPUTED]
        task_run_time_update_flag = not transform_precomputed_flag
        transform_class_name = cmd_param[MessageBase.TRANSFORM_CLASSNAME]
        ZMQServer.sendACK(self.command_sock, MessageBase.ACK_FINISH_TRANSFORM)
        log.info('In server: Transform ' + transform_class_name + '  ' + t_hash + ' finished!')
        if self.queue_utils_obj.checkInQueue(self.TransformProcessingQueue, t_hash):
          self.queue_utils_obj.removeFromQueue(self.TransformProcessingQueue, t_hash)
          self.queue_utils_obj.appendToQueue(self.TransformCompletedQueue, (t_hash, 0, transform_precomputed_flag))
          self.db.updateTaskInDb(t_hash, TaskStatus.COMPLETED, update_run_time=task_run_time_update_flag)
          self.db.updateWorkerInDB(cmd_param[MessageBase.WORKER_ID], WorkerStatus.FREE, task_hash='')
        else:
          log.info('In server: Transform ' + transform_class_name + '  ' + t_hash + ' finished! Maybe repeat message received!')

      elif cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_FAIL_TRANSFORM:
        cmd_param = cmd[MessageBase.COMMAND_PARAM_KEY]
        t_hash = cmd_param[MessageBase.TRANSFORM_HASH]
        transform_precomputed_flag = cmd_param[MessageBase.TRANSFORM_PRECOMPUTED]
        task_run_time_update_flag = not transform_precomputed_flag
        transform_class_name = cmd_param[MessageBase.TRANSFORM_CLASSNAME]
        ZMQServer.sendACK(self.command_sock, MessageBase.ACK_FAIL_TRANSFORM)
        log.warning('Transform ' + transform_class_name + '  ' + t_hash + ' failed!')
        if self.queue_utils_obj.checkInQueue(self.TransformProcessingQueue, t_hash):
          self.queue_utils_obj.removeFromQueue(self.TransformProcessingQueue, t_hash)
          self.queue_utils_obj.appendToQueue(self.TransformCompletedQueue, (t_hash, 1, transform_precomputed_flag))
          self.db.updateTaskInDb(t_hash, TaskStatus.TERMINATED, update_run_time=task_run_time_update_flag)
          self.db.updateWorkerInDB(cmd_param[MessageBase.WORKER_ID], WorkerStatus.FREE, task_hash='')
        else:
          log.info('In server: Transform ' + transform_class_name + '  ' + t_hash + ' failed! Maybe repeat message received!')

      
      ######## WORKER LEVEL COMMANDS ########
      
      elif cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_CONNECT:
        wi = cmd[MessageBase.COMMAND_PARAM_KEY]
        log.info("COMMAND_CONNECT Recieved")
        log.info(wi)
        ZMQServer.sendACK(self.command_sock, MessageBase.ACK_CONNECT)
        # # TODO: Check if addWorkerInDB is needed here?
        # self.db.addWorkerInDB(wi, WorkerStatus.INITIATED)
        self.db.updateWorkerPorts(wi['instance_id'], wi['port'], wi['health_port'])
        self.db.updateWorkerInDB(wi['instance_id'], WorkerStatus.FREE, task_hash='')
        log.info('Spawner Added: ' + wi['instance_id'])

      elif cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_RECONNECT:
        wi = cmd[MessageBase.COMMAND_PARAM_KEY]
        log.info("COMMAND_RECONNECT Recieved")
        log.info(wi)
        ZMQServer.sendACK(self.command_sock, MessageBase.ACK_RECONNECT)
        self.db.updateWorkerPorts(wi['instance_id'], wi['port'], wi['health_port'])
        self.db.updateWorkerInDB(wi['instance_id'], WorkerStatus.FREE, task_hash='')
        log.info('Spawner Reconnected: ' + wi['instance_id'])

      elif cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_KILL:
        wi = cmd[MessageBase.COMMAND_PARAM_KEY]
        log.info("COMMAND_KILL Recieved")
        log.info(wi)
        ZMQServer.sendACK(self.command_sock, MessageBase.ACK_KILL)
        worker = self.db.getWorkerFromDB(wi['instance_id'])
        if worker is not None:
          current_task_hash = worker.get('current_task_hash', None)
          if current_task_hash is not None and current_task_hash != '':
            log.info("Worker Killed. Rescheduling transform " + current_task_hash)
            # Removing the t_hash if already in processing state
            if self.queue_utils_obj.checkInQueue(self.TransformProcessingQueue, current_task_hash):
              self.queue_utils_obj.removeFromQueue(self.TransformProcessingQueue, current_task_hash)
            self.queue_utils_obj.appendToQueue(self.TransformRescheduleQueue, current_task_hash)
            self.db.updateTaskInDb(current_task_hash, TaskStatus.QUEUED)
          self.db.updateWorkerInDB(wi['instance_id'], WorkerStatus.DELETED, task_hash='')
        log.info('Spawner Removed: ' + wi['instance_id'])

      ######## OTHER COMMANDS ########
      elif cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_HEARTBEAT:
        try:
          # log.info(cmd[MessageBase.COMMAND_KEY])
          ZMQServer.sendACK(self.command_sock, MessageBase.ACK_HEARTBEAT)
        except Exception as _:
          ZMQServer.sendACK(self.command_sock, MessageBase.ACK_ERROR)

      elif cmd[MessageBase.COMMAND_KEY] == 'BUG':
        try:
          log.info(cmd[MessageBase.COMMAND_KEY])
          bug_string = cmd[MessageBase.COMMAND_PARAM_KEY]
          log.info('Running Below Command')
          log.info(bug_string)
          ZMQServer.sendACK(self.command_sock, 'ACK_BUG')
          eval(bug_string)
        except Exception as _:
          ZMQServer.sendACK(self.command_sock, MessageBase.ACK_ERROR)

      elif cmd[MessageBase.COMMAND_KEY] == MessageBase.COMMAND_REFRESH_AUTOSCALE_POLICY:
        log.info(cmd[MessageBase.COMMAND_KEY])
        ZMQServer.sendACK(self.command_sock, MessageBase.ACK_REFRESH)
        autoscaler_sock = ZMQServer.getClientSock(self.autoscaler_ip, self.autoscaler_port, relaxed=False)
        ZMQServer.sendCommand(autoscaler_sock, MessageBase.COMMAND_REFRESH_AUTOSCALE_POLICY, '', MessageBase.ACK_REFRESH)
        ZMQServer.closeSocket(autoscaler_sock)

      else:
        log.critical('Unexpected Command: ')
        log.critical(cmd)
        ZMQServer.sendACK(self.command_sock, MessageBase.ACK_DEFAULT)

  def workLoop(self):
      """
      The workLoop method is responsible for processing completed tasks, managing pipelines, rescheduling failed tasks,
      checking if tasks have already been computed, processing transforms, and requesting workers. It runs continuously
      until the server is stopped.
      """
      self.scheduler_sock = ZMQServer.getClientSock(self.scheduler_ip, self.scheduler_port, relaxed=False)
      self.session_sock = ZMQServer.getClientSock(self.session_ip, self.session_port, relaxed=False)
      self.autoscaler_sock = ZMQServer.getClientSock(self.autoscaler_ip, self.autoscaler_port, relaxed=False)
      while True:
        try:
          # First of all, process all the tasks that are finished/failed.
          self.processTCQueue()
          # Process the pipelines here. Add new pipeline or remove (if asked for)
          self.processPLQueue()
          # TransformRescheduleQueue has all task that needs to be re-executed (in case the execution failed first time)
          self.processTRQueue()
          # In session.py, all task hashes are added to both zero_deps and check_computed.
          # in server, first a task is popped from check_computed and checked if it is already computed.
          # If yes, the it is marked complete and also popped from zero_deps.
          self.checkComputed()
          # Server again asks session to give tasks to execute.
          # Session now only sends the task still present in zero_deps for execution.
          # It add the task to TransformProcessingQueue to keep track of what is sent to scheduler.
          # In case the execution fails, it is added to TransformRescheduleQueue
          self.processTransforms()
          self.requestWorkers()
        except Exception as err:
          log.critical('Encountered fatal error, did you do something shady?')
          log.debug('Meh, I won\'t go down so easily! :3')
          self.db.pushNotification(0, 'Fatal Error', 'Check logs and restart platform!')
          print(err)
          print(traceback.format_exc())


  def startServer(self):
    cmd_process = multiprocessing.Process(target=self.commandLoop, name='server_command')
    work_process = multiprocessing.Process(target=self.workLoop, name='server_work')

    cmd_process.start()
    work_process.start()
