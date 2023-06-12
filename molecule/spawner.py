import argparse
import yaml
import json
import signal
import sys
import os
import traceback
import time
import multiprocessing
import subprocess

from .resources import setEnvVars, TaskStatus, WorkerStatus, InstanceMetadata
from .zmqserver import ZMQServer, MessageBase
from . import pl_storage
from . import pl_logger as logger
from .database import Database

log = logger.getLogger('spawner', enable_cloud_logging=False)


class Spawner:
  """
  The Spawner class is responsible for spawning and managing worker instances.
  It initializes the worker instance with the required parameters and adds it to the platform.
  It also provides methods to check if a task has been computed and to synchronize inputs.
  """
  def __init__(self, in_port, in_health_port, command_server, store_loc, db, remote_ip=None, remote_store_loc=None,
           docker_config=None, spawner_type=None, offline=False, storage_type='nfs'):
    """
    Initializes the Spawner object with the given parameters.

    Args:
      in_port (int): The port number on which the worker instance will listen for incoming requests.
      in_health_port (int): The port number on which the worker instance will listen for health check requests.
      command_server (str): The IP address and port number of the command server in the format "IP:PORT".
      store_loc (str): The local directory path where the worker instance will store its data.
      db (Database): The database object used to store and retrieve task information.
      remote_ip (str, optional): The IP address of the remote storage server. Defaults to None.
      remote_store_loc (str, optional): The remote directory path where the worker instance will store its data. Defaults to None.
      docker_config (str, optional): The path to the Docker configuration file. Defaults to None.
      spawner_type (str, optional): The type of spawner. Defaults to None.
      offline (bool, optional): Whether the worker instance is offline. Defaults to False.
      storage_type (str, optional): The type of storage. Defaults to 'nfs'.
    """
    
    self.in_port = str(in_port)
    self.in_health_port = str(in_health_port)
    self.spawner_type = spawner_type

    self.command_ip, self.command_port = command_server.split(':')
    
    self.manager = multiprocessing.Manager()
    self.worker_info = self.manager.dict()
    
    self.im = InstanceMetadata()
    self.store_loc = store_loc
    self.remote_ip = remote_ip
    self.remote_store_loc = remote_store_loc

    self.exec_process = None
    self.health_process = None

    self.alive = multiprocessing.Value('i', 0)
    self.status =  multiprocessing.Value('i',  WorkerStatus.FREE)
    self.last_heartbeat_epoch = multiprocessing.Value('i', int(time.time()))

    self.storage_type = storage_type
    self.db = db

    self.docker_map = None
    if docker_config is not None:
      # with open(docker_config, 'r') as docker_cfg:
      #   self.docker_map = yaml.load(docker_cfg, Loader=yaml.Loader)
      self.docker_map = json.loads(docker_config)

    self.store = pl_storage.SpawnerStorage(store_loc, remote_ip, remote_store_loc, mode=storage_type, db=self.db)

    if offline:
      self.in_ip = 'localhost'
    else:
      self.in_ip = self.im.getInstanceIp()

  def addToPlatform(self, in_ip, in_port, in_health_port, spawner_type):
    """
    Adds the worker instance to the platform by initializing it with the required parameters.
    It sets the worker_info dictionary with the instance details and sends a command to the server to connect.
    
    Args:
    - in_ip (str): The IP address of the worker instance.
    - in_port (str): The port number of the worker instance.
    - in_health_port (str): The health port number of the worker instance.
    - spawner_type (str): The type of spawner used to spawn the worker instance.
    """
    self.worker_info['instance_id'] = self.im.getInstanceId()
    self.worker_info['instance_name'] = self.im.getInstanceName()
    self.worker_info['zone'] = self.im.getInstanceZone()
    self.worker_info['ip'] = in_ip
    self.worker_info['port'] = in_port
    self.worker_info['health_port'] = in_health_port
    self.worker_info['affinity'] = spawner_type
    self.worker_info['gce_machine_type'] = self.im.getMachineType()
    self.worker_info['disk_size'] = self.im.getDiskSize()
    self.worker_info['cpu'] = self.im.getCpuCount()
    self.worker_info['memory'] = self.im.getMemory()
    self.worker_info['gpu'] = self.im.getGpuCount()
    self.worker_info['gpu_type'] = self.im.getGpuType()
    log.info(spawner_type + ' spawner ' + in_ip + ':' + in_port + ' connecting to server')
    out_sock = ZMQServer.getClientSock(self.command_ip, self.command_port)
    ZMQServer.sendCommand(out_sock, MessageBase.COMMAND_CONNECT, dict(self.worker_info), MessageBase.ACK_CONNECT)
    ZMQServer.closeSocket(out_sock)

  @staticmethod
  def checkComputed(ti, store):
    """
    Checks if the task has been computed by verifying if the output hashes are present in the store.
    
    Args:
    - ti (dict): The task instance dictionary.
    - store (pl_storage.Storage): The storage object used to store the output hashes.
    
    Returns:
    - computed_flag (bool): A boolean value indicating if the task has been computed or not.
    """
    computed_flag = True
    for _, output_hash in ti['hashes']['outputs'].items():
      if store.check(os.path.join(ti['data_dir'], output_hash)) is False:
        computed_flag = False
        break
    return computed_flag

  @staticmethod
  def syncInputs(ti, store):
    """
    Synchronizes the input files for a given task instance with the storage object.
    If the input file is a dictionary, it synchronizes each value in the dictionary.
    
    Args:
    - ti (dict): The task instance dictionary.
    - store (pl_storage.Storage): The storage object used to store the input files.
    """
    for input_name, input_hash in ti['hashes']['inputs'].items():
      if type(input_hash) is dict:
        for in_ser, in_hash in input_hash.items():
          store.sync(in_hash)
      else:
        store.sync(input_hash)

  def run(self, t_hash):
    """
    Runs the specified task with the given hash.

    Args:
        t_hash (str): The hash of the task to run.

    Returns:
        int: 0 if the task was successfully completed, 1 otherwise.
    """
    ti = self.db.getTaskFromDb(t_hash)
    log.info('Processing ' + ti['class_name'])
    
    store = pl_storage.SpawnerStorage(self.store_loc, mode=self.storage_type, db=self.db)
    print(ti['working_dir'])
    log.info('pulling code')
    local_code_loc = store.pullCode(ti['working_dir'])
    log.debug(local_code_loc)

    computed_flag = self.checkComputed(ti, store)
    log.info('COMPUTED_FLAG = ' + str(computed_flag))

    if computed_flag:
      ti_dict = {
        MessageBase.TRANSFORM_HASH: t_hash,
        MessageBase.TRANSFORM_PRECOMPUTED: computed_flag,
        MessageBase.TRANSFORM_CLASSNAME: ti['class_name'],
        MessageBase.WORKER_ID: self.worker_info['instance_id']
      }
      log.info('In spawner: Transform ' + ti['class_name'] + ' finished!')
      ZMQServer.sendCommand(self.out_sock, MessageBase.COMMAND_FINISH_TRANSFORM,
                            ti_dict, MessageBase.ACK_FINISH_TRANSFORM)
      return 0
    
    package_loc = os.path.dirname(os.path.abspath(__file__))
    log.debug('package_loc = ' + package_loc)

    ti_hash = ti['hashes']['transform_hash']
    ti_dict = ti['hashes']
    ti_dict['data_dir'] = ti['data_dir']
    self.db.saveTransform(ti_dict)
    working_dir = local_code_loc

    cmd = ""
    docker_prefix, store_loc = self.getDockerCmdPrefix(ti, '/mnt/data/molecule/code')

    if docker_prefix is None:
      log.critical('Docker not found')
      ti_dict = {
        MessageBase.TRANSFORM_HASH: t_hash,
        MessageBase.TRANSFORM_PRECOMPUTED: computed_flag,
        MessageBase.TRANSFORM_CLASSNAME: ti['class_name'],
        MessageBase.WORKER_ID: self.worker_info['instance_id']
      }
      log.info('In spawner: Transform ' + ti['class_name'] + ' failed!')
      ZMQServer.sendCommand(self.out_sock, MessageBase.COMMAND_FAIL_TRANSFORM,
                            ti_dict, MessageBase.ACK_FAIL_TRANSFORM)
      return 1

    log.info("Adding output dataset to DB")
    if not self.db.addTransformHashesToDb(ti):
      log.warn("Adding output hash to dataset DB failed")
    
    if ti['class_language'] == 'py':
      cmd = docker_prefix + f"{sys.executable} {package_loc}/executors/py/executor.py -t " + ti_hash + " -s " + store_loc + " --storage-type " + self.storage_type + " -ttl " + str(ti['ttl'])  + " --dev-env " + self.db.db_env + " -wd " + working_dir
    elif ti['class_language'] == 'r':
      cmd = docker_prefix + f"Rscript {package_loc}/executors/r/executor.R -t " + ti_hash + " -s " + store_loc + " --storage-type " + self.storage_type + " -ttl " + str(ti['ttl']) + " --dev-env " + self.db.db_env + " -wd " + working_dir
    elif ti['class_language'] == 'hive':
      cmd = f"{sys.executable} {package_loc}/executors/hive/executor.py -t " + ti_hash + " -s " + store_loc + " --storage-type " + self.storage_type + " -ttl " + str(ti['ttl']) + " --dev-env " + self.db.db_env
    elif ti['class_language'] == 'bq':
      cmd = f"{sys.executable} {package_loc}/executors/bq/executor.py -t " + ti_hash + " -s " + store_loc + " --storage-type " + self.storage_type + " -ttl " + str(ti['ttl']) + " --dev-env " + self.db.db_env

    log.debug('Executing command: ' + str(cmd))

    cmd = cmd.split(" ")
    out = subprocess.run(cmd, cwd=os.getcwd())

    log.info('Sending Success/Failure to Platform')

    ti_dict = {
      MessageBase.TRANSFORM_HASH: t_hash,
      MessageBase.TRANSFORM_PRECOMPUTED: computed_flag,
      MessageBase.TRANSFORM_CLASSNAME: ti['class_name'],
      MessageBase.WORKER_ID: self.worker_info['instance_id']
    }

    is_computed = TaskStatus.COMPLETED
    if out.returncode != 0:
      is_computed = TaskStatus.TERMINATED

    all_upd_resp = True
    for _, output_hash in ti['hashes']['outputs'].items():
      upd_resp =  self.db.updateDatasetHash(os.path.join(ti['data_dir'], output_hash), is_computed)
      if not upd_resp:
        all_upd_resp = upd_resp
    
    if not all_upd_resp:
      log.critical('Updating DatasetDB failed')
      ZMQServer.sendCommand(self.out_sock, MessageBase.COMMAND_FAIL_TRANSFORM, 
                            ti_dict, MessageBase.ACK_FAIL_TRANSFORM)
      return 1


    if out.returncode == 0:
      log.info('In spawner: Transform ' + ti['class_name'] + ' finished!')
      ZMQServer.sendCommand(self.out_sock, MessageBase.COMMAND_FINISH_TRANSFORM,
                            ti_dict, MessageBase.ACK_FINISH_TRANSFORM)
      return 0

    # Need to differentiate between executor failing due to sigint or otherwise
    # If we kill spawner, COMMAND_FAIL_TRANSFORM should not be sent
    elif out.returncode == -2:
      log.info('Return Code = ' + str(out.returncode))
      log.info('Exiting executor due to sigint')
      return -2
    else:
      # In case where there the executor really failed, send fail transform command
      log.info('Return Code = ' + str(out.returncode))
      log.info('In spawner: Transform ' + ti['class_name'] + ' failed!')
      ZMQServer.sendCommand(self.out_sock, MessageBase.COMMAND_FAIL_TRANSFORM,
                            ti_dict, MessageBase.ACK_FAIL_TRANSFORM)
      return out.returncode

  def fail(self, t_hash):
    """
    Sends a command to mark a task as failed.

    Args:
      t_hash (str): The hash of the task to mark as failed.

    Returns:
      None
    """
    # Not sure where the self.run failed. It maybe possible that compute hash is already present.
    # Default assuming that computed_flag is True as we do not want to update run_time of task in DB.
    computed_flag = True
    ti = self.db.getTaskFromDb(t_hash)
    ti_dict = {
      MessageBase.TRANSFORM_HASH: t_hash,
      MessageBase.TRANSFORM_PRECOMPUTED: computed_flag,
      MessageBase.TRANSFORM_CLASSNAME: ti['class_name'],
      MessageBase.WORKER_ID: self.worker_info['instance_id']
    }
    ZMQServer.sendCommand(self.out_sock, MessageBase.COMMAND_FAIL_TRANSFORM, ti_dict, MessageBase.ACK_FAIL_TRANSFORM)
    return

  def getDockerCmdPrefix(self, ti, working_dir):
    """
    Returns the Docker command prefix to execute a task in a Docker container.

    Args:
      ti (dict): The task information dictionary.
      working_dir (str): The working directory path.

    Returns:
      tuple: A tuple containing the Docker command prefix and the store location.
    """
    if self.docker_map is not None:
      if ti['docker_container'] is not None:
        dc_info = self.docker_map.get(ti['docker_container'], None)
      else:
        dc_info = self.docker_map.get(ti['class_language'], None)
      if dc_info is None:
        return None, self.store_loc
      else:
        dc_name = dc_info['dc_name']
        store_loc = self.store_loc
        return 'docker exec -w ' + working_dir + \
          ' -e GOOGLE_CLOUD_PROJECT=' + os.environ['GOOGLE_CLOUD_PROJECT'] + \
          ' -e GOOGLE_SERVICE_ACCOUNT=' + os.environ['GOOGLE_SERVICE_ACCOUNT'] + \
          ' -e MOLECULE_BUCKET_NAME=' + os.environ['MOLECULE_BUCKET_NAME'] + \
          ' -e MOLECULE_UI_URL=' + os.environ['MOLECULE_UI_URL'] + \
          ' ' + dc_name + ' ', store_loc
    else:
      return '', self.store_loc

  def listenForCommand(self):
    """
    Listens for incoming commands on the input socket and processes them accordingly.
    """
    cmd = ZMQServer.getMessage(self.in_sock)
    cmd_str = cmd[MessageBase.COMMAND_KEY]
    cmd_param = cmd[MessageBase.COMMAND_PARAM_KEY]

    if cmd_str == MessageBase.COMMAND_HEARTBEAT:
      ZMQServer.sendACK(self.in_sock, MessageBase.ACK_HEARTBEAT)
    elif cmd_str == MessageBase.COMMAND_ALLOCATE_WORKER:
      log.info('Got COMMAND_ALLOCATE_WORKER: ')
      print(cmd)
      ZMQServer.sendACK(self.in_sock, MessageBase.ACK_ALLOCATE_WORKER)
      with self.status.get_lock():
        self.status.value = WorkerStatus.BUSY
      self.out_sock = ZMQServer.getClientSock(self.command_ip, self.command_port)

      # Trying to run the task now
      run_return_code = None
      retries = 1
      while run_return_code == None and retries <= 3:
        retries += 1
        try:
          run_return_code = self.run(cmd_param)
        except Exception as err:
          log.warning('Running task in spawner failed for retry = ' + str(retries))
          log.critical(err)
          log.critical(traceback.format_exc())
          run_return_code = None
      # If still the run_return_code is None, then communicate to server.
      if run_return_code == None:
        log.warning('Running task in spawner failed for all retries. Sending fail command to server.')
        self.fail(cmd_param)

      ZMQServer.closeSocket(self.out_sock)
      self.out_sock = None
      with self.status.get_lock():
        self.status.value = WorkerStatus.FREE
    else:
      ZMQServer.sendACK(self.in_sock, MessageBase.ACK_ERROR)

  def executorLoop(self):
    """
    Listens for incoming commands on the input socket and processes them accordingly.
    """
    self.in_sock = ZMQServer.getServerSock(self.in_port)
    
    while True:
      log.debug('Listening for commands...')
      self.listenForCommand()

  def healthMonitorLoop(self):
    """
    Monitors the health of the worker instance by listening for incoming commands on the health socket.
    If a heartbeat command is received, updates the last heartbeat epoch and sends an ACK_HEARTBEAT response.
    If a reconnect command is received and the worker is free, reconnects to the command server and sends an ACK_RECONNECT response.
    Otherwise, sends an ACK_ERROR response.
    """
    self.health_sock = ZMQServer.getServerSock(self.in_health_port)

    while True:
      cmd = ZMQServer.getMessage(self.health_sock)
      cmd_str = cmd[MessageBase.COMMAND_KEY]

      current_epoch = int(time.time())
      if cmd_str == MessageBase.COMMAND_HEARTBEAT:
        with self.last_heartbeat_epoch.get_lock():
          self.last_heartbeat_epoch.value = current_epoch
        if self.alive.value == 1:
          ZMQServer.sendACK(self.health_sock, MessageBase.ACK_HEARTBEAT)
        else:
          ZMQServer.sendACK(self.health_sock, MessageBase.ACK_ERROR)
      elif cmd_str == MessageBase.COMMAND_RECONNECT:
        with self.last_heartbeat_epoch.get_lock():
          self.last_heartbeat_epoch.value = current_epoch
        if self.alive.value == 1 and self.status.value == WorkerStatus.FREE:
          log.info('Reconnecting to command server...')
          out_sock = ZMQServer.getClientSock(self.command_ip, self.command_port)
          ZMQServer.sendCommand(out_sock, MessageBase.COMMAND_RECONNECT, dict(self.worker_info), MessageBase.ACK_RECONNECT)
          ZMQServer.closeSocket(out_sock)
          ZMQServer.sendACK(self.health_sock, MessageBase.ACK_RECONNECT)
        else:
          log.info('Trying to reconnect a dead or busy worker. Worker Status = ' + str(self.status.value))
          ZMQServer.sendACK(self.health_sock, MessageBase.ACK_ERROR)
      else:
        ZMQServer.sendACK(self.health_sock, MessageBase.ACK_ERROR)


  def destructLoop(self):
    """
    Monitors the last heartbeat epoch of the worker instance and kills the spawner if it has not received a heartbeat
    command in SELF_KILL_SECONDS seconds. Runs in a separate process.
    """
    SELF_KILL_SECONDS = 900
    while True:
      # log.info('destructLoop = ' + str(self.last_heartbeat_epoch.value))
      if (int(time.time()) - self.last_heartbeat_epoch.value) > SELF_KILL_SECONDS:
        self.destruct()
      time.sleep(180)

  def startExecutor(self):
    """
    Starts the executor loop, health monitor loop, and destruct loop in separate processes.
    Sets the alive flag to 1 to indicate that the worker is alive.
    Adds the worker to the platform by sending a message to the command server with the worker's IP address,
    input port, health port, and spawner type.
    """
    with self.alive.get_lock():
      self.alive.value = 1
    self.exec_process = multiprocessing.Process(target=self.executorLoop)
    self.exec_process.start()
    self.health_process = multiprocessing.Process(target=self.healthMonitorLoop)
    self.health_process.start()
    self.destruct_process = multiprocessing.Process(target=self.destructLoop)
    self.destruct_process.start()
    self.addToPlatform(self.in_ip, self.in_port, self.in_health_port, self.spawner_type)

  def destruct(self, delete=True):
    """
    Kills the spawner instance. If delete is True, the instance is deleted from the cloud platform. Otherwise, the instance
    is shut down without deleting it. The method executes the appropriate command based on the value of delete.
    """
    log.info("Killing the spawner from self")
    if delete:
      log.info("Deleting instance")
      cmd = "gcloud compute instances delete " + self.im.getInstanceName()  + " --zone " + self.im.getInstanceZone() + " -q "
    else:
      log.info("Just shutting down. Not deleting")
      cmd = "sudo shutdown"
    log.info(cmd)
    os.popen(cmd)

def signal_handler(sig, frame):
  """
  Handles the SIGINT signal (Ctrl+C) by stopping all spawners and terminating their processes.
  """
  log.critical("You pressed Ctrl+C")
  signal.signal(signal.SIGINT, original_sigint)
  for spawn in spawners:
    log.info('Stopping the spawner...')
    print(yaml.dump(spawn.worker_info, indent=2))
    spawn.alive = False
    out_sock = ZMQServer.getClientSock(spawn.command_ip, spawn.command_port)
    ZMQServer.sendCommand(out_sock, MessageBase.COMMAND_KILL, dict(spawn.worker_info), MessageBase.ACK_KILL)
    ZMQServer.closeSocket(out_sock)
    out_sock = None
    spawn.exec_process.terminate()
    spawn.health_process.terminate()
  sys.exit(0)

def main():
  """
  The main function that initializes and starts the spawner instances.
  """
  multiprocessing.set_start_method('fork')

  arg_parser = argparse.ArgumentParser(description='utility for running spawner')
  arg_parser.add_argument('-c', '--server-config', action='store', help='Path to server configuration variables')
  arg_parser.add_argument('-cs', '--command-server', action='store', help='Platform/Command Server Address')
  arg_parser.add_argument('-p', '--ports', action='store', help='Ports for running the executor(comma-separated)')
  arg_parser.add_argument('-hpp', '--health-port-prefix', action='store', help='health port prefix for the given ports')
  arg_parser.add_argument('-s', '--store-loc', action='store', help='Store location for files')
  arg_parser.add_argument('-mt', '--machine-type', action='store', help='Machine type, cpu, gpu, custom')
  arg_parser.add_argument('-rip', '--remote-ip', action='store', help='Remote IP for online storage')
  arg_parser.add_argument('-rsl', '--remote-store-loc', action='store', help='Remote storage location')
  arg_parser.add_argument('-dc', '--docker-config', action='store', help='Docker Config File')
  arg_parser.add_argument('-o', '--offline', action='store_true', help='Connect to central online platform')
  arg_parser.add_argument('--storage-type', action='store', help='nfs or gcs storage')

  args = arg_parser.parse_args()
  
  # Loading configuration from file
  config = None
  if args.server_config is not None:
    setEnvVars(args.server_config)
    with open(args.server_config, 'r') as fl:
      config = yaml.load(fl, Loader=yaml.Loader)

  if args.offline:
    os.environ['MONGO_URI'] = 'mongodb://localhost:27017'

  if args.storage_type is None:
    args.storage_type = 'gcs'
  if args.health_port_prefix is None:
    args.health_port_prefix = '1'

  # Setting deployment type label
  deployment_type_label = os.environ['DEPLOYMENT_TYPE_LABEL']

  db = Database(deployment_type_label)

  if args.command_server is None:
    ip, port = db.getPlatformConfig(deployment_type_label)
    args.command_server = str(ip) + ':' + str(port)

  log.info('Arguments: ' + '\n' + yaml.dump(vars(args), indent=2))

  ports = args.ports.split(',')
  for port in ports:
    health_port = ''.join([args.health_port_prefix, port])
    assert int(port) <= 9999, "Port should be [1-9999]"
    assert int(port) != int(health_port), "Health Port should be different than Spawner Port"
    assert int(health_port) <= 65535, "Health Port should be within [10-65535]"

  original_sigint = signal.getsignal(signal.SIGINT)
  signal.signal(signal.SIGINT, signal_handler)

  spawners = []
  for port in ports:
    spawner = Spawner(port, ''.join([args.health_port_prefix, port]), args.command_server, args.store_loc, db,
                      remote_ip=args.remote_ip, remote_store_loc=args.remote_store_loc,
                      docker_config=args.docker_config, spawner_type=args.machine_type,
                      offline=args.offline, storage_type=args.storage_type)
    spawner.startExecutor()
    spawners.append(spawner)

  for s in spawners:
    s.exec_process.join()
    s.health_process.join()
    s.destruct_process.join()

if __name__ == '__main__':
  main()
