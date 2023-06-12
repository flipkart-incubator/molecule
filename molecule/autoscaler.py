import uuid
import time
import pytz
import multiprocessing
from datetime import datetime

from .queue_utils import QueueUtils, MultiProcessPriorityQueue
from .resources import WorkerStatus
from .zmqserver import ZMQServer, MessageBase
from .database import Database
from .instance_managers import CloudPlatformInstanceManager
from . import pl_logger as logger

LOCAL_TZ = pytz.timezone('Asia/Kolkata')
log = logger.getLogger('autoscaler', enable_cloud_logging=False)

# Example req_worker_type spec
# req_worker_type = {
#   'cpu': '8',
#   'memory': '16G',
#   'disk_size': '100G',
#   'gpu': '1',
#   'affinity': 'cpu',
# }
# 
# Example worker_info spec
# worker_info = {
#   'instance_id': self.im.getInstanceId(),
#   'ip': in_ip,
#   'instance_name': self.im.getInstanceName(),
#   'zone': self.im.getInstanceZone(),
#   'port': in_port,
#   'health_port': in_health_port,
#   'affinity': spawner_type,
#   'gce_machine_type': self.im.getMachineType(),
#   'disk_size': self.im.getDiskSize(),
#   'cpu': self.im.getCpuCount(),
#   'memory': self.im.getMemory(),
#   'gpu': self.im.getGpuCount(),
#   'gpu_type': self.im.getGpuType()
# }


class AutoScalingPolicy:
  def __init__(self, db=None, db_env='stage'):
    """
    Initializes an instance of the AutoScalingPolicy class.

    Args:
        db (Database): An instance of the Database class.

    Attributes:
        db (Database): An instance of the Database class.
        minimum_workers (int): The minimum number of workers.
        maximum_workers (int): The maximum number of workers.
        min_workers_per_affinity (dict): The minimum number of workers per affinity.
        max_workers_per_affinity (dict): The maximum number of workers per affinity.
        base_worker_for_affinity (dict): The base worker configuration for each affinity.
        scaling_factor (int): The scaling factor.
        scale_down_after (int): The time after which a worker is marked for scale down.
        max_cpus (int): The maximum number of CPUs.
        max_memory (str): The maximum amount of memory.
        max_disk_size (str): The maximum disk size.
        max_gpus (int): The maximum number of GPUs.
        quota_max_gpus (int): The maximum number of GPUs for quota.
        mark_unhealthy_after (int): The time after which a worker is marked as unhealthy.

    """
    self.db = db
    self.db_env = db_env

    self.minimum_workers = 1
    self.maximum_workers = 4
    self.min_workers_per_affinity = {
      'hive': 0,
      'cpu': 1,
      'gpu': 0
    }
    self.max_workers_per_affinity = {
      'hive': 2,
      'cpu': 4,
      'gpu': 1
    }
    self.base_worker_for_affinity = {
      'cpu': {
        'cpu': '4',
        'memory': '16G',
        'disk_size': '256G',
        'gpu': '0',
        'affinity': 'cpu'
      },
      'hive': {
        'cpu': '2',
        'memory': '8G',
        'disk_size': '256G',
        'gpu': '0',
        'affinity': 'hive'
      },
      'gpu': {
        'cpu': '16',
        'memory': '60G',
        'disk_size': '256G',
        'gpu': '1',
        'affinity': 'gpu'
      },
    }
    self.scaling_factor = 1 # 1 worker per 1 request
    self.scale_down_after = 300 # 5 minutes
    self.max_cpus = 32
    self.max_memory = '208G'
    self.max_disk_size = '1000G'
    self.max_gpus = 1
    self.quota_max_gpus = 8
    self.mark_unhealthy_after = 300 # 5 minutes

    self.configurePolicyFromDB()

  def configurePolicyFromConfig(self, config):
    """
    Configures the policy from a configuration file.

    Args:
        config (str): The path to the configuration file.

    """
    pass

  def configurePolicyFromDB(self):
    """
    Configures the policy from the database.

    """
    policy = Database.getAutoscalingPolicy(self.db_env)
    if policy is not None:
      self.minimum_workers = policy['minimum_workers']
      self.maximum_workers = policy['maximum_workers']
      self.min_workers_per_affinity = policy['min_workers_per_affinity']
      self.max_workers_per_affinity = policy['max_workers_per_affinity']
      self.scaling_factor = policy['scaling_factor']
      self.scale_down_after = policy['scale_down_after']
      self.mark_unhealthy_after = policy['mark_unhealthy_after']
      self.max_cpus = policy['max_cpus']
      self.max_memory = policy['max_memory']
      self.max_disk_size = policy['max_disk_size']
      self.max_gpus = policy['max_gpus']

  def checkWorkerReqPolicy(self, req_worker_type):
    """
    Checks if a worker request satisfies the policy constraints.

    Args:
        req_worker_type (dict): A dictionary containing the worker request.

    Returns:
        bool: True if the worker request satisfies the policy constraints, False otherwise.

    """
    # if req_worker_type is within the max constraints, return True
    # else return False
    if int(req_worker_type['cpu']) > self.max_cpus:
      return False
    if int(req_worker_type['memory'].split('G')[0]) > int(self.max_memory.split('G')[0]):
      return False
    if int(req_worker_type['disk_size'].split('G')[0]) > int(self.max_disk_size.split('G')[0]):
      return False
    if int(req_worker_type['gpu']) > self.max_gpus:
      return False
    return True

  def checkPolicy(self, req_worker_type, queued_workers):
    """
    Checks if a worker request can be fulfilled based on the current active and queued workers.

    Args:
        req_worker_type (dict): A dictionary containing the worker request.
        queued_workers (list): A list of queued workers.

    Returns:
        bool: True if the worker request can be fulfilled, False otherwise.

    """
    active_workers = self.db.getAllWorkersFromDB([WorkerStatus.INITIATED, WorkerStatus.CREATED, WorkerStatus.FREE, WorkerStatus.BUSY])
    if (len(active_workers) + len(queued_workers)) >= self.maximum_workers:
      return False

    if req_worker_type.get('exact_machine_type', '') != '':
      return True

    # if req_worker_type's affinity is less than max_workers_per_affinity, return True
    # else return False
    affinity = req_worker_type['affinity']
    affinity_active_workers = [w for w in active_workers if w['affinity'] == affinity]
    affinity_queued_workers = [w for w in queued_workers if w['affinity'] == affinity]
    if (len(affinity_active_workers) + len(affinity_queued_workers)) >= self.max_workers_per_affinity[affinity]:
      return False

    self.checkWorkerReqPolicy(req_worker_type)
    return True

  def checkScaleDown(self, running_workers, policy='default'):
    """
    Determines which workers should be scaled down based on a given policy.

    Args:
        running_workers (list): A list of running workers.
        policy (str): The scaling policy to use.

    Returns:
        list: A list of workers to be scaled down.

    """
    scale_down_workers = []
    if policy == 'default':
      # policy where the largest free worker is scaled down first
      # if runnning workers are more than minimum_workers, scale down
      # check for each affinity
      for affinity in self.min_workers_per_affinity:
        affinity_workers = [w for w in running_workers if w['affinity'] == affinity]
        if len(affinity_workers) <= self.min_workers_per_affinity[affinity]:
          pass
        else:
          free_workers = [w for w in affinity_workers if w['status'] == WorkerStatus.FREE]
          scale_down_free_workers = []
          for w in free_workers:
            if (datetime.now(LOCAL_TZ) - w['mark_free_timestamp'].astimezone(LOCAL_TZ)).seconds > self.scale_down_after:
              scale_down_free_workers.append(w)
          possible_scale_down = max(len(affinity_workers) - self.min_workers_per_affinity[affinity], 0)
          possible_scale_down = min(possible_scale_down, len(scale_down_free_workers))
          # sort free_workers by gpu, cpu, memory, disk_size
          scale_down_free_workers = sorted(scale_down_free_workers, key=lambda k: (int(k['gpu']), int(k['cpu']),
                                                            int(k['memory'].split('G')[0]), int(k['disk_size'].split('G')[0])),
                                reverse=True)
          if possible_scale_down>0:
            scale_down_workers = [*scale_down_workers, *scale_down_free_workers[:possible_scale_down]]
    else:
      # policy where any free worker larger than base_worker_per_affinity is scaled down
      free_workers = [w for w in running_workers if w['status'] == WorkerStatus.FREE]
      for w in free_workers:
        if (datetime.now(LOCAL_TZ) - w['mark_free_timestamp'].astimezone(LOCAL_TZ)).seconds > self.scale_down_after:
          # now check if w is greater than base_worker_per_affinity in terms of gpu, cpu, memory, disk_size
          if int(w['gpu']) > int(self.base_worker_for_affinity[w['affinity']]['gpu']):
            scale_down_workers.append(w)
          elif int(w['cpu']) > int(self.base_worker_for_affinity[w['affinity']]['cpu']):
            scale_down_workers.append(w)
          elif int(w['memory'].split('G')[0]) > int(self.base_worker_for_affinity[w['affinity']]['memory'].split('G')[0]):
            scale_down_workers.append(w)
          elif int(w['disk_size'].split('G')[0]) > int(self.base_worker_for_affinity[w['affinity']]['disk_size'].split('G')[0]):
            scale_down_workers.append(w)

    return scale_down_workers


  def checkScaleUp(self, active_workers, queued_workers):
    """
    Checks if any active workers can be scaled up to meet the minimum number of workers per affinity.

    Args:
        active_workers (list): A list of active workers.
        queued_workers (list): A list of queued workers.

    Returns:
        list: A list of workers to be scaled up.

    """
    # Check if any active workers can be scaled up
    # min_workers_per_affinity is the minimum number of workers
    scale_up_workers = []
    for affinity in self.min_workers_per_affinity:
      active_affinity_workers = [w for w in active_workers if w['affinity'] == affinity]
      queued_affinity_workers = [w for w in queued_workers if w['affinity'] == affinity]
      scale_up_affinity_workers = [w for w in scale_up_workers if w['affinity'] == affinity]
      while len(active_affinity_workers) + len(queued_affinity_workers) + len(scale_up_affinity_workers) < self.min_workers_per_affinity[affinity]:
        scale_up_workers.append(self.base_worker_for_affinity[affinity])
        scale_up_affinity_workers.append(self.base_worker_for_affinity[affinity])
    return scale_up_workers


  def getProvisionModel(self, affinity):
    """
    Returns the provisioning model for a given affinity.

    Args:
        affinity (str): The affinity.

    Returns:
        str: The provisioning model.

    """
    # Return the provisioning model
    return 'SPOT'
  
  
class Autoscaler:
  def __init__(self, in_port, server_ip, server_port, platforms, autoscaling_policy, db):
    """
    Initializes the Autoscaler class.

    Args:
        in_port (int): The port number.
        server_ip (str): The server IP address.
        server_port (int): The server port number.
        autoscaling_policy (AutoScalingPolicy): The autoscaling policy.
        db (Database): The database.

    """
    self.in_port = in_port
    self.server_ip = server_ip
    self.server_port = server_port
    self.platforms = platforms
    self.db = db
    self.asp = autoscaling_policy
    self.cpim = CloudPlatformInstanceManager(platforms=self.platforms)
    self.removeDaemonWorkers()

    if len(self.platforms) > 1:
      self.platform = 'auto'
    else:
      self.platform = self.platforms[0]

    # Autoscaler needs to maintain a list of workers in DB
    # But doesn't take actions automatically

    self.manager = multiprocessing.Manager()
    self.lock = self.manager.Lock()
    self.queue_lock = self.manager.Lock()
    self.queue_utils_obj = QueueUtils(self.queue_lock)
    self.createWorkerQueue = MultiProcessPriorityQueue(self.manager)
    self.initiatedWorkerQueue = self.manager.list()
    self.removeWorkerQueue = self.manager.list()
    # To track if server is up or not.
    self.server_up = multiprocessing.Value('i', 1)

  def removeDaemonWorkers(self):
    """
    Removes the workers running on GCP.

    """
    workers = self.db.getAllWorkersFromDB([WorkerStatus.INITIATED, WorkerStatus.CREATED, WorkerStatus.FREE, WorkerStatus.BUSY, WorkerStatus.STALE])
    for worker in workers:
      self.cpim.deleteInstance(worker['instance_name'], worker['zone'], platform=worker['platform'])
      self.db.deleteWorkerFromDB(worker['instance_id'])

  def createWorkers(self):
    """
    Creates a new worker of the given type and adds it to the database with an 'init' status.
    The method creates a worker on GCP and adds it to the database. It also adds the worker to the initiated worker queue.

    """
    while self.createWorkerQueue.getLen() > 0:
      self.lock.acquire()
      req_worker_type, _ = self.createWorkerQueue.popFromQueue()
      self.lock.release()
      affinity = req_worker_type['affinity']
      if self.platform == 'auto':
        platform = self.cpim.getBestPlatform(req_worker_type)
      else:
        platform = self.platform
      if req_worker_type.get('exact_machine_type', '') != '':
        machine_type, zone, cpus, memory = (req_worker_type['exact_machine_type'], 'asia-south1-a', 1, '2G')
      else:
        machine_type, zone, cpus, memory = self.cpim.getMachineTypeZoneInfo(req_worker_type, platform=platform)
      provision_model = self.asp.getProvisionModel(affinity)
      instance_name = '{}-{}-spawner-{}-{}'.format(self.cpim.getDeploymentType(platform=platform), affinity, provision_model.lower(), str(uuid.uuid4()).split('-')[0])
      disk_size_in_gb = int(req_worker_type['disk_size'].replace('G', ''))
      # create worker on GCP
      # TODO: check returncode
      if self.cpim.createInstance(instance_name, zone, machine_type, disk_size_in_gb, affinity, provision_model, platform=platform):
        self.lock.acquire()
        log.debug('Adding instance {} to DB'.format(instance_name))
        # add worker to DB
        # TODO: adjust for GPU
        instance_id = str(self.cpim.getInstanceID(instance_name, zone, platform=platform))
        worker_info = {
          'instance_id': instance_id,
          'instance_name': instance_name,
          'zone': zone,
          'ip': self.cpim.getInstanceIP(instance_name, zone, platform=platform),
          'affinity': affinity,
          'gce_machine_type': machine_type,
          'disk_size': req_worker_type['disk_size'],
          'cpu': cpus,
          'memory': memory,
          'gpu': 0,
          'gpu_type': None,
          'platform': platform
        }
        self.db.addWorkerInDB(worker_info, WorkerStatus.INITIATED)
        self.queue_utils_obj.appendToQueue(self.initiatedWorkerQueue, instance_id)
        log.debug('Added instance {} to DB'.format(instance_name))
        self.lock.release()
      else:
        log.critical("Worker creation failed for instance: {}".format(instance_name))


  def removeWorkers(self):
      """
      This method removes workers from the system. It pops a worker from the removeWorkerQueue and marks it as deleted in the database.
      It then removes the worker from GCP.

      """
      while len(self.removeWorkerQueue) > 0:
        # Marks worker as deleted from DB
        worker_id = self.queue_utils_obj.popFromQueue(self.removeWorkerQueue)
        self.db.updateWorkerInDB(worker_id, WorkerStatus.DELETED)
        # Remove the worker from GCP
        worker_info = self.db.getWorkerFromDB(worker_id)
        # TODO: check returncode
        self.cpim.deleteInstance(worker_info['instance_name'], worker_info['zone'], platform=worker_info['platform'])

  def scaleUp(self):
    """
    This method checks if there are less workers than the policy and adds them to the createWorkerQueue if true.

    """
    self.lock.acquire()
    active_workers = self.db.getAllWorkersFromDB([WorkerStatus.INITIATED, WorkerStatus.CREATED, WorkerStatus.FREE, WorkerStatus.BUSY])
    scale_up_worker_types = self.asp.checkScaleUp(active_workers, self.createWorkerQueue.flatten())
    if len(scale_up_worker_types)>0:
      log.info("scaleUp worker requested: {}".format(len(scale_up_worker_types)))
    for worker_type in scale_up_worker_types:
      self.createWorkerQueue.appendToQueue(worker_type, 2)
    self.lock.release()

  def scaleDown(self):
    """
    This method checks if there are more workers than the policy and if the difference between mark_free_timestamp and current_timestamp is greater than scale_down_after.
    If true, it adds the worker to the removeWorkerQueue.

    """
    self.lock.acquire()
    running_workers = self.db.getAllWorkersFromDB([WorkerStatus.FREE, WorkerStatus.BUSY])
    scale_down_workers = self.asp.checkScaleDown(running_workers)
    for worker in scale_down_workers:
      self.db.updateWorkerInDB(worker['instance_id'], WorkerStatus.DELETED)
      self.queue_utils_obj.appendToQueue(self.removeWorkerQueue, worker['instance_id'])
    self.lock.release()

  def getFreeWorker(self, req_worker_type):
    """
    This method checks if a worker of the given type is free. If a free worker is found, it returns the worker's instance ID. 
    If no free worker is found, it returns None.

    Args:
    - req_worker_type: A dictionary containing the specifications of the requested worker type.

    Returns:
    - The instance ID of a free worker of the requested type, if available. Otherwise, returns None.
    """
    if req_worker_type['affinity'] == 'hive':
      asp_hive_worker_type = self.asp.base_worker_for_affinity['hive']
      matching_workers = self.db.getMatchingWorkersFromDB(
        asp_hive_worker_type['cpu'], asp_hive_worker_type['memory'], asp_hive_worker_type['gpu'],
        req_worker_type['disk_size'], req_worker_type['affinity'], WorkerStatus.FREE)
    elif req_worker_type.get('exact_machine_type', '') != '':
      matching_workers = self.db.getExactMachineTypeWorkersFromDB(req_worker_type['exact_machine_type'], WorkerStatus.FREE)
    else:
      matching_workers = self.db.getMatchingWorkersFromDB(
        req_worker_type['cpu'], req_worker_type['memory'], req_worker_type['gpu'], req_worker_type['disk_size'],
        req_worker_type['affinity'], WorkerStatus.FREE
      )
    if len(matching_workers)>0:
      # sort by cpu and memory
      matching_workers = sorted(matching_workers, key=lambda k: (k['cpu'], k['memory']))
      # return the first match
      return matching_workers[0]['instance_id']
    else:
      return None

  def checkAvailableWorkers(self, req_worker_type):
    """
    This method checks if a worker of the given type is available. It checks if the quota is available and if a worker of the given type is in the createWorkerQueue.

    Args:
    - req_worker_type: A dictionary containing the specifications of the requested worker type.

    Returns:
    - True if a worker of the given type is available. Otherwise, returns False.
    """
    if req_worker_type['affinity'] == 'hive':
      asp_hive_worker_type = self.asp.base_worker_for_affinity['hive']
      available_workers = self.db.getMatchingWorkersFromDB(
        asp_hive_worker_type['cpu'], asp_hive_worker_type['memory'], asp_hive_worker_type['gpu'],
        req_worker_type['disk_size'], req_worker_type['affinity']
      )
    elif req_worker_type.get('exact_machine_type', '') != '':
      available_workers = self.db.getExactMachineTypeWorkersFromDB(req_worker_type['exact_machine_type'])
    else:
      available_workers = self.db.getMatchingWorkersFromDB(
        req_worker_type['cpu'], req_worker_type['memory'], req_worker_type['gpu'], req_worker_type['disk_size'],
        req_worker_type['affinity']
      )
    if len(available_workers)>0:
      return True
    else:
      if self.createWorkerQueue.checkInQueue(req_worker_type):
        return True
      else:
        return False

  def listenForCommand(self):
    """
    This method listens for incoming commands from the server and processes them accordingly.
    """
    cmd = ZMQServer.getMessage(self.in_sock)
    cmd_str = cmd[MessageBase.COMMAND_KEY]
    cmd_param = cmd[MessageBase.COMMAND_PARAM_KEY]

    if cmd_str == MessageBase.REQUEST_NEW_FREE_WORKER:
      # Server requests a free worker of some type, given some parameters
      # Autoscaler checks if the worker of given type is free
      # if free, return the worker
      # else checks via the Autoscaling policy if a new worker is needed
      # if new worker is not needed, waits for a free worker
      # if new worker is needed, creates a new worker and returns it
      # it also marks the returned worker busy
      # log.info('requested new free worker')
      self.lock.acquire()
      req_worker_type, queue = cmd_param
      worker_id = self.getFreeWorker(req_worker_type)
      if worker_id is not None:
        log.info('worker assigned {}'.format(worker_id))
        self.db.updateWorkerInDB(worker_id, WorkerStatus.BUSY)
        ZMQServer.sendResponse(self.in_sock, MessageBase.WORKER_ASSIGNED, worker_id)
      elif self.asp.checkPolicy(req_worker_type, self.createWorkerQueue.flatten()):
        log.info('REQUEST_NEW_FREE_WORKER: worker creation request {}'.format(req_worker_type['affinity']))
        if req_worker_type['exact_machine_type'] != '':
          log.info('REQUEST_NEW_FREE_WORKER: machine type {}'.format(req_worker_type['exact_machine_type']))
        self.createWorkerQueue.appendToQueue(req_worker_type, queue)
        ZMQServer.sendResponse(self.in_sock, MessageBase.WORKER_CREATION_QUEUED, None)
      else:
        # log.info('worker policy wait')
        ZMQServer.sendResponse(self.in_sock, MessageBase.WORKER_POLICY_WAIT, None)
      self.lock.release()

    elif cmd_str == MessageBase.REQUEST_FREE_WORKER:
      # log.info('requested free worker')
      self.lock.acquire()
      req_worker_type, queue = cmd_param
      if self.checkAvailableWorkers(req_worker_type):
        # log.info('worker available')
        worker_id = self.getFreeWorker(req_worker_type)
        if worker_id is not None:
          log.info('worker assigned {}'.format(worker_id))
          self.db.updateWorkerInDB(worker_id, WorkerStatus.BUSY)
          ZMQServer.sendResponse(self.in_sock, MessageBase.WORKER_ASSIGNED, worker_id)
        else:
          # log.info('worker policy wait')
          ZMQServer.sendResponse(self.in_sock, MessageBase.WORKER_POLICY_WAIT, None)
      else:
        # log.info('worker not available')
        ZMQServer.sendResponse(self.in_sock, MessageBase.WORKER_NOT_AVAILABLE, None)
      self.lock.release()

    elif cmd_str == MessageBase.COMMAND_REFRESH_AUTOSCALE_POLICY:
      log.info('Refreshing Autoscaling Policy')
      ZMQServer.sendACK(self.in_sock, MessageBase.ACK_REFRESH)
      self.asp.configurePolicyFromDB()

    else:
      ZMQServer.sendResponse(self.in_sock, MessageBase.ACK_ERROR, None)

  def commandLoop(self):
    """
    Listens for incoming commands on the specified port and processes them accordingly.
    """
    self.in_sock = ZMQServer.getServerSock(self.in_port)
    while True:
      self.listenForCommand()

  def workLoop(self):
    """
    This method is responsible for managing the worker nodes in the autoscaler. It continuously checks if any worker is free,
    and if so, checks if it is needed. If the worker is not needed, it scales down the number of workers. If the worker is needed,
    it marks it as busy. This method runs as long as the server is up, and scales down and removes workers when the server health is down.
    """
    while self.server_up.value == 1:
      # Check if any worker is free
      # if free, check if it is needed
      # if not needed, scale down
      # if needed, mark it busy
      self.createWorkers()
      self.removeWorkers()
      self.scaleDown()
      self.scaleUp()
    log.warn('Exiting Autoscaler workLoop as server health is down')
    log.info('Scaling Down and Removing Workers')
    self.removeDaemonWorkers()

  def serverHealthMonitorLoop(self):
    """
    Monitors the health of the server by sending a heartbeat message to the server at regular intervals.
    If the server does not respond with an ACK_HEARTBEAT message within a certain time period, the server is considered unhealthy.
    If the server remains unhealthy for a specified amount of time, the server_up flag is set to 0, indicating that the server is down.
    """
    server_sock = ZMQServer.getClientSock(self.server_ip, self.server_port)
    WAIT_FOR_SERVER_UP_SECONDS = 600
    last_server_health_check = int(time.time())
    while self.server_up.value == 1:
      current_epoch = int(time.time())
      response = ZMQServer.sendCommand(server_sock, MessageBase.COMMAND_HEARTBEAT, '', MessageBase.ACK_HEARTBEAT)
      if response == MessageBase.ACK_HEARTBEAT:
        last_server_health_check = current_epoch
      else:
        log.warn('Server unhealthy')

      if (current_epoch - last_server_health_check) > WAIT_FOR_SERVER_UP_SECONDS:
        with self.server_up.get_lock():
          self.server_up.value = 0
        log.warn('Server unhealthy since seconds = ' + str(WAIT_FOR_SERVER_UP_SECONDS))
        break

      time.sleep(120)
    log.warn('Exiting Autoscaler serverHealthMonitorLoop as server health is down')


  def healthMonitorLoop(self):
    """
    Monitors the health of workers and updates their status accordingly.
    """
    stale_machines_dict = {}
    busy_unhealthy_machines_dict = {}
    STALE_TO_DELETE_WORKER_SECONDS = 600
    STALE_TO_FREE_WORKER_SECONDS = 0
    BUSY_TO_DELETE_WORKER_SECONDS = 3600

    while self.server_up.value == 1:
      self.lock.acquire()
      workers = self.db.getAllWorkersFromDB([WorkerStatus.FREE, WorkerStatus.BUSY, WorkerStatus.STALE])
      self.lock.release()
      while len(workers) > 0 :
        worker_instance_id = self.queue_utils_obj.popFromQueue(workers)['instance_id']
        # Get latest from DB again. As some other process might have updated it.
        wi = self.db.getWorkerFromDB(worker_instance_id)
        worker_ip = wi['ip']
        worker_port = wi['port']
        worker_health_port = wi['health_port']
        worker_status = wi['status']

        worker_sock = ZMQServer.getClientSock(worker_ip, worker_port)
        worker_health_sock = ZMQServer.getClientSock(worker_ip, worker_health_port)

        response_health_sock = ZMQServer.sendCommand(worker_health_sock, MessageBase.COMMAND_HEARTBEAT, None, MessageBase.ACK_HEARTBEAT)

        now_timestamp = datetime.now().astimezone()
        # State Switch Plan
        # Busy either goes to DELETE or stays BUSY from here. It gets FREE from server only.
        if worker_status == WorkerStatus.BUSY:
          # Just for health sock as worker sock is supposed to not respond anyways. It is supposed to be running the job.
          if response_health_sock != MessageBase.ACK_HEARTBEAT:
            if worker_instance_id in busy_unhealthy_machines_dict.keys():
              seconds_till_last_health_check = (now_timestamp - busy_unhealthy_machines_dict[worker_instance_id]).seconds
              if seconds_till_last_health_check > BUSY_TO_DELETE_WORKER_SECONDS:
                # Potentially the code fo spawner got into error and spawner cannot self-destruct.
                # Kill the spawner so that any task stuck with it is freed.
                # In case, spawner self-destructs before, it won't come into health-check.
                log.info("Killing from autoscaler on behalf of spawner. Spawner unreachable for more than " + str(BUSY_TO_DELETE_WORKER_SECONDS) + " seconds")
                server_sock = ZMQServer.getClientSock(self.server_ip, self.server_port)
                ZMQServer.sendCommand(server_sock, MessageBase.COMMAND_KILL, wi, MessageBase.ACK_KILL)
                ZMQServer.closeSocket(server_sock)
                self.queue_utils_obj.appendToQueue(self.removeWorkerQueue, worker_instance_id)
                del busy_unhealthy_machines_dict[worker_instance_id]
              else:
                # Maybe busy in some task and health port is also not responding. Wait for more time before deleting.
                pass
            else:
              # First time health check failed
              busy_unhealthy_machines_dict[worker_instance_id] = now_timestamp
          else:
            # if present in busy_unhealthy_machines_dict, then remove. Status remains unchanged from here.
            # Also, no reconnect needed as we never disconnected it or marked it stale.
            if worker_instance_id in busy_unhealthy_machines_dict.keys():
              del busy_unhealthy_machines_dict[worker_instance_id]

        elif worker_status == WorkerStatus.FREE:
          # Check for worker sock as well. Not just health sock. Both should respond
          if response_health_sock == MessageBase.ACK_HEARTBEAT:
            response_worker_sock = ZMQServer.sendCommand(worker_sock, MessageBase.COMMAND_HEARTBEAT, None, MessageBase.ACK_HEARTBEAT)
          else:
            response_worker_sock = ''
          if response_worker_sock == MessageBase.ACK_HEARTBEAT:
            # All Good. Do nothing. Remove from stale dicts if present
            if worker_instance_id in stale_machines_dict.keys():
              del stale_machines_dict[worker_instance_id]
            if worker_instance_id in busy_unhealthy_machines_dict.keys():
              del busy_unhealthy_machines_dict[worker_instance_id]
          else:
            # Mark Stale if any of the two health check is not responding
            stale_machines_dict[worker_instance_id] = now_timestamp
            self.db.pushNotification(1, 'Spanwer Disconnected', worker_ip + ':' + worker_port)
            self.lock.acquire()
            self.db.updateWorkerInDB(worker_instance_id, status=WorkerStatus.STALE)
            self.lock.release()

        elif worker_status == WorkerStatus.STALE:
          # Check for worker sock as well. Not just health sock. Both should respond.
          if response_health_sock == MessageBase.ACK_HEARTBEAT:
            response_worker_sock = ZMQServer.sendCommand(worker_sock, MessageBase.COMMAND_HEARTBEAT, None, MessageBase.ACK_HEARTBEAT)
          else:
            response_worker_sock = ''
          if response_worker_sock == MessageBase.ACK_HEARTBEAT:
            # Mark Free again
            seconds_till_last_health_check = (now_timestamp - stale_machines_dict.get(worker_instance_id, now_timestamp)).seconds
            if seconds_till_last_health_check > STALE_TO_FREE_WORKER_SECONDS:
              if worker_instance_id in stale_machines_dict.keys():
                del stale_machines_dict[worker_instance_id]
              log.info('Reconnecting spawner ' + worker_ip + ':' + worker_port)
              ZMQServer.sendCommand(worker_health_sock, MessageBase.COMMAND_RECONNECT, None, MessageBase.ACK_RECONNECT)
              self.db.pushNotification(2, 'Spanwer Reconnected', worker_ip + ':' + worker_port)
            else:
              # Wait for more successful health check before unmarking stale
              pass
          else:
            # Check if we already marked it stale some time back
            if worker_instance_id in stale_machines_dict.keys():
              seconds_till_last_health_check = (now_timestamp - stale_machines_dict[worker_instance_id]).seconds
              if seconds_till_last_health_check > STALE_TO_DELETE_WORKER_SECONDS:
                # Spawner not responding. Kill the spawner. So that a new can be created.
                log.info("Killing from autoscaler on behalf of spawner. Spawner unreachable for more than " + str(STALE_TO_DELETE_WORKER_SECONDS) + " seconds")
                server_sock = ZMQServer.getClientSock(self.server_ip, self.server_port)
                ZMQServer.sendCommand(server_sock, MessageBase.COMMAND_KILL, wi, MessageBase.ACK_KILL)
                ZMQServer.closeSocket(server_sock)
                self.queue_utils_obj.appendToQueue(self.removeWorkerQueue, worker_instance_id)
                del stale_machines_dict[worker_instance_id]
              else:
                # Already marked stale. Do nothing and wait for some for time to become active/delete.
                pass
        ZMQServer.closeSocket(worker_health_sock)
        ZMQServer.closeSocket(worker_sock)
      time.sleep(60)
    log.warn('Exiting Autoscaler healthMonitorLoop as server health is down')

  def startAutoscaler(self):
    """
    Starts the autoscaler by creating and starting four separate processes:
    - `commandLoop`: listens for commands from the autoscaler controller
    - `workLoop`: creates and removes workers as needed based on the autoscaling policy
    - `healthMonitorLoop`: monitors the health of the workers and removes any that are unhealthy
    - `serverHealthMonitorLoop`: monitors the health of the server and takes action if necessary

    This method should be called after the `Autoscaler` object has been initialized.
    """
    cmd_process = multiprocessing.Process(target=self.commandLoop, name='autoscaler_command')
    work_process = multiprocessing.Process(target=self.workLoop, name='autoscaler_work')
    health_process = multiprocessing.Process(target=self.healthMonitorLoop, name='autoscaler_health')
    server_health_process = multiprocessing.Process(target=self.serverHealthMonitorLoop, name='server_health')
    cmd_process.start()
    work_process.start()
    health_process.start()
    server_health_process.start()

