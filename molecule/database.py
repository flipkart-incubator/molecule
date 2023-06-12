import os
import sys
import yaml
import hashlib
import traceback
import pytz
from datetime import datetime, timedelta
import multiprocessing
from pymongo import MongoClient

sys.path.append(os.getcwd())
from molecule.graph import generateGraph
from molecule import pl_logger as logger
from molecule.resources import TaskStatus, WorkerStatus

log = logger.getLogger('database', enable_cloud_logging=False)
LOCAL_TZ = pytz.timezone('Asia/Kolkata')

yaml.Dumper.ignore_aliases = lambda *args : True

class NoDatesFullLoader(yaml.Loader):
  @classmethod
  def remove_implicit_resolver(cls, tag_to_remove):
    if not 'yaml_implicit_resolvers' in cls.__dict__:
      cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

    for first_letter, mappings in cls.yaml_implicit_resolvers.items():
      cls.yaml_implicit_resolvers[first_letter] = [(tag, regexp)
                                                    for tag, regexp in mappings
                                                    if tag != tag_to_remove]

NoDatesFullLoader.remove_implicit_resolver('tag:yaml.org,2002:timestamp')


class Database:
  """
  A class representing a database connection.

  Attributes:
  - client (MongoClient): A MongoClient object representing the database client.
  - db (Database): A database object representing the database.
  - db_env (str): The name of the database environment.
  - expire_days (int): The number of days after which a pipeline will expire.
  - manager (multiprocessing.Manager): A multiprocessing Manager object.
  - lock (multiprocessing.Lock): A multiprocessing Lock object.
  - _worker_cache (multiprocessing.Manager.dict): A multiprocessing Manager dictionary object representing the worker cache.
  """
  @staticmethod
  def get_common_db(db_env):
    """
    Returns a MongoClient object and a database object for the given database environment.

    Args:
    - db_env (str): The name of the database environment.

    Returns:
    - tuple: A MongoClient object and a database object.
    """
    if os.environ.get('MONGO_URI'):
      client = MongoClient(os.environ.get('MONGO_URI'))
    else:
      client = MongoClient('mongodb://mongo0:27017,mongo1:27017/')
    db = client[db_env]
    return client, db
  
  def __init__(self, db_env='stage', rebuild=False):
    """
    Initializes a Database object.

    Args:
    - db_env (str): The name of the database environment.
    - rebuild (bool): Whether to rebuild the worker cache or not.
    """
    if os.environ.get('MONGO_URI'):
      self.client = MongoClient(os.environ.get('MONGO_URI'))
    else:
      self.client = MongoClient('mongodb://mongo0:27017,mongo1:27017/')
    self.db = self.client[db_env]
    self.db_env = db_env
    self._createDefaultProject()
    self._createIndexes()
    self.expire_days = 150
    self.manager = multiprocessing.Manager()
    self.lock = self.manager.Lock()
    if rebuild:
      self._worker_cache = self.manager.dict()
      self._rebuildCache()
    
  def _rebuildCache(self):
    """
    Rebuilds the worker cache.
    """
    self._worker_cache = self.manager.dict()
    for doc in self.db['workers'].find():
      self._worker_cache[doc['_id']] = self.manager.dict(doc)
    
  def _createIndexes(self):
    """
    Creates indexes for the collections in the database.
    """
    self.db['pipelines'].create_index([('timestamp', -1)])
    self.db['pipelines'].create_index([('pinned', 1), ('timestamp', -1)])
    self.db['pipelines'].create_index([('username', 1), ('pinned', 1), ('timestamp', -1)])
    self.db['pipelines'].create_index([('status', 1)])
    self.db['projects'].create_index([('timestamp', -1)])
    self.db['tasks'].create_index([('status', 1)])
    self.db['notifications'].create_index([('timestamp', -1)])
  
  def _createDefaultProject(self):
    """
    Creates a default project if it does not exist in the database.
    """
    if self.db['projects'].find_one({'_id': 'default'}) is None:
      self.db['projects'].insert_one({
        '_id': 'default',
        'name': 'default',
        'user': 'root',
        'timestamp': datetime.now().astimezone(LOCAL_TZ),
        'status': 0,
        'scheduled': 0,
        'runs': dict(),
        'notes': '',
        'working_dir': '',
        'pipeline_name': '',
        'pipeline_spec_loc': '',
        'pipeline_config': '',
        'update_config': 0,
        'command': '',
        'schedule_str': ''
      })
    
  def close(self):
    """
    Closes the MongoClient object.
    """
    self.client.close()
    
  @staticmethod
  def getPlatformConfig(db_env='stage'):
    """
    Returns the IP address and port number of the platform.

    Args:
    - db_env (str): The name of the database environment.

    Returns:
    - tuple: The IP address and port number of the platform.
    """
    client, db = Database.get_common_db(db_env)
    ip_port_dict = db['config'].find_one({'_id': 'platform'})
    client.close()
    if ip_port_dict is not None:
      ip, port = ip_port_dict['ip'], ip_port_dict['port']
      return ip, port
    else:
      return 'localhost', '5566'
    
  @staticmethod
  def setPlatformConfig(db_env, ip, port):
    """
    Sets the IP address and port number of the platform.

    Args:
    - db_env (str): The name of the database environment.
    - ip (str): The IP address of the platform.
    - port (str): The port number of the platform.
    """
    client, db = Database.get_common_db(db_env)
    db['config'].update_one({'_id': 'platform'}, {'$set': {'ip': ip, 'port': port}}, upsert=True)
    client.close()
  
  @staticmethod
  def getAutoscalingPolicy(db_env):
    """
    Returns the autoscaling policy.

    Returns:
    - dict: The autoscaling policy.
    """
    client, db = Database.get_common_db(db_env)
    doc = db['config'].find_one({'_id': 'autoscaling'})
    client.close()
    if doc is not None:
      return doc
    else:
      return None


  ######## TRANSFORM DB FUNCTIONS ########
  @staticmethod
  def getUserDatahash(path):
    """
    Returns the user and dataset hash from the given path.

    Args:
    - path (str): The path to extract the user and dataset hash from.

    Returns:
    - user (str): The user extracted from the path.
    - dataset_hash (str): The dataset hash extracted from the path.
    """
    path_split = path.split('/store/data/')
    user = path_split[0].split("/")[-1]
    user = user.replace(".", "")
    dataset_hash = ""
    if len(path_split) > 1:
      dataset_hash = path_split[1]
    return user, dataset_hash 

  @staticmethod
  def prefix_collection_name(db_env, collection_name):
    """
    Prefixes the given collection name with the database environment name.

    Args:
    - db_env (str): The name of the database environment.
    - collection_name (str): The name of the collection.

    Returns:
    - str: The prefixed collection name.
    """
    return db_env  + '_' + collection_name

  def loadTransform(self, t_hash):
      """
      Load a transform from the database.

      Args:
      - t_hash (str): The hash of the transform to load.

      Returns:
      - dict: The transform document from the database.

      Raises:
      - Exception: If the transform does not exist in the database.
      """
      doc = self.db['transforms'].find_one({'_id': t_hash})
      if doc is None:
        raise Exception(t_hash + " does not exists in transforms")
      return doc
  
  def saveTransform(self, t_hash_dict):
    """
    Saves a transform to the database.

    Args:
    - t_hash_dict (dict): A dictionary containing the transform hash and other metadata.

    Returns:
    - None

    Raises:
    - None
    """
    t_hash_dict['expires_at'] = datetime.now().astimezone(LOCAL_TZ) + timedelta(days=self.expire_days)
    self.db['transforms'].update_one({'_id': t_hash_dict['transform_hash']}, {'$set': t_hash_dict}, upsert=True)
  
  ######## DATASET DB FUNCTIONS ########
  

  def addTransformHashesToDb(self, ti):
    """
    Adds the transform hashes to the database.

    Args:
    - ti (dict): A dictionary containing the transform information.

    Returns:
    - bool: True if the transform hashes were added successfully, False otherwise.
    """
    user, _ = Database.getUserDatahash(os.path.join(ti['data_dir'], ti['hashes']['transform_hash']))  
    for _, output_hash in ti['hashes']['outputs'].items():
      hash_key = output_hash + "_" + user
      log.info("Adding {} dataset in DB".format(hash_key))
      curr_time = datetime.now().astimezone(LOCAL_TZ)
      t_data = {
        '_id':  hash_key,
        'path': os.path.join(ti['data_dir'], output_hash),
        # 'project': data['project'],
        'user': user,
        'status': TaskStatus.PROCESSING,
        'timestamp': curr_time,
        'expires_at': curr_time + timedelta(days=self.expire_days),
        'ti_type': ti['name'],
        'ttl':  ti['ttl'] 
      }
      try:
        self.db['datasets'].update_one({'_id': hash_key}, {'$set': t_data}, upsert=True)
      except Exception as err:
        log.warning('Failed addingTransformHashtoDB')
        print(err)
        print(traceback.format_exc())
        return False
    return True

  def checkDatasetHash(self, dataset_hash): 
    """
    Checks if the dataset hash exists in the database.

    Args:
    - dataset_hash (str): The hash of the dataset.

    Returns:
    - bool: True if the dataset hash exists in the database and is not expired, False otherwise.
    """
    user, data_hash = Database.getUserDatahash(dataset_hash)
    hash_key = data_hash + "_" + user
    doc = self.db['datasets'].find_one({'_id': hash_key})
    if doc is None:
      return False
    else:
      diff_creation = datetime.now().astimezone(LOCAL_TZ) - doc['timestamp'].astimezone(LOCAL_TZ)
      if doc['status'] == TaskStatus.COMPLETED and diff_creation.days <= doc['ttl']:
        return True
      else:
        return False

  def getDatasetTTL(self, dataset_hash):
    """
    Gets the time-to-live (TTL) of the dataset hash.

    Args:
    - dataset_hash (str): The hash of the dataset.

    Returns:
    - int: The TTL of the dataset hash, or -1 if the dataset hash does not exist in the database.
    """
    user, data_hash = Database.getUserDatahash(dataset_hash)
    hash_key = data_hash + "_" + user
    doc = self.db['datasets'].find_one({'_id': hash_key})
    if doc is None:
      return -1
    else:
      return doc['ttl']

  def updateDatasetHash(self, dataset_hash, status):
    """
    Updates the status of the dataset hash in the database.

    Args:
    - dataset_hash (str): The hash of the dataset.
    - status (str): The new status of the dataset hash.

    Returns:
    - bool: True if the dataset hash was updated successfully, False otherwise.
    """
    user, data_hash = Database.getUserDatahash(dataset_hash)
    hash_key = data_hash + "_" + user
    set_dict = {'status': status}
    try:
      curr_time = datetime.now().astimezone(LOCAL_TZ)
      set_dict['timestamp'] = curr_time
      set_dict['expires_at'] = curr_time + timedelta(days=self.expire_days)
      
      self.db['datasets'].update_one({'_id': hash_key}, {'$set': set_dict})
      return True
    except Exception as err:
      log.warning('Failed updateDatasetInDb')
      print(err)
      print(traceback.format_exc())
      return False

  def deleteDatasetHash(self, dataset_hash):
    """
    Deletes the dataset hash from the database.

    Args:
    - dataset_hash (str): The hash of the dataset.

    Returns:
    - bool: True if the dataset hash was deleted successfully, False otherwise.
    """
    user, data_hash = Database.getUserDatahash(dataset_hash)
    hash_key = data_hash + "_" + user
    doc = self.db['datasets'].find_one({'_id': hash_key})
    if doc is None:
      print("No such dataset hash")
      return False
    try:
      set_dict = {'status': TaskStatus.STALE} 
      self.db['datasets'].update_one({'_id': hash_key}, {'$set': set_dict})
      return True
    except Exception as err:
      log.warning('Failed Deletion of Dataset')
      print(err)
      print(traceback.format_exc())
      return False

    
  ######## PIPELINE DB FUNCTIONS ########

  def addPipelineToDb(self, data):
    """
    Adds a pipeline to the database.

    Args:
    - data (dict): A dictionary containing the pipeline data.

    Returns:
    - bool: True if the pipeline was added successfully, False otherwise.
    """
    s_data = {
      '_id': data['hash'],
      'name': data['name'],
      'pipeline': data['pipeline'],
      'hash': data['hash'],
      'config': data['config'],
      'user': data['user'],
      'graph': generateGraph(data['task_dict']).toSigmaJS(),
      'project': data['project'],
      'pinned': 0,
      'expires_at': datetime.now().astimezone(LOCAL_TZ) + timedelta(days=self.expire_days),
      'status': TaskStatus.QUEUED
    }
    if self.db['pipelines'].find_one({'_id': data['hash']}) is None or data['message'] != '':
      s_data['message'] = data['message']
    s_data = yaml.load(yaml.dump(s_data), Loader=NoDatesFullLoader)
    s_data = {
      **s_data,
      'timestamp': datetime.now().astimezone(LOCAL_TZ)
    }
    try:
      self.db['pipelines'].update_one({'_id': data['hash']}, {'$set': s_data}, upsert=True)
      project_ref = self.db['projects'].find_one({'name': data['project']})
      if project_ref is None:
        project_ref_id = "default"
      else:
        project_ref_id = project_ref['_id']
      update_dict = {
        'runs.'+str(data['hash']): datetime.now().astimezone(LOCAL_TZ)
      }
      self.db['projects'].update_one({'_id': project_ref_id}, {'$set': update_dict})
      return True
    except Exception as err:
      log.warning('Failed addPipelineToDb')
      print(err)
      print(traceback.format_exc())
      return False
  
  def updatePipelineInDb(self, p_hash, status):
    """
    Updates the status of a pipeline in the database.

    Args:
    - p_hash (str): The hash of the pipeline to update.
    - status (TaskStatus): The new status of the pipeline.

    Returns:
    - bool: True if the pipeline was updated successfully, False otherwise.
    """
    try:
      curr_status = self.db['pipelines'].find_one({'_id': p_hash})['status']
      if curr_status == TaskStatus.TERMINATED and status == TaskStatus.PROCESSING:
        return True
      self.db['pipelines'].update_one({'_id': p_hash}, {'$set': {'status': status}})
      return True
    except Exception as err:
      log.warning('Failed updatePipelineInDb')
      print(err)
      print(traceback.format_exc())
      return False

  ######## TASK DB FUNCTIONS ########
  
  def addTaskToDb(self, t_hash, ti):
    """
    Adds a task to the database.

    Args:
    - t_hash (str): The hash of the task to add.
    - ti (TaskInstance): The task instance to add.

    Returns:
    - bool: True if the task was added successfully, False otherwise.
    """
    # First check, if task is already in processing state in DB.
    # If yes, do not add again to DB as we lose information in that process.
    task_from_db = self.getTaskFromDb(t_hash)
    if task_from_db != None:
      if task_from_db.get('status', None) == TaskStatus.PROCESSING:
        return True

    if type(ti) != dict:
      ti_dict = yaml.load(yaml.dump(ti.serializeDict()), Loader=NoDatesFullLoader)
    else:
      ti_dict = ti
    curr_time = datetime.now().astimezone(LOCAL_TZ)
    t_data = {
      '_id': t_hash,
      'status': TaskStatus.QUEUED,
      'timestamp': curr_time,
      'expires_at': curr_time + timedelta(days=self.expire_days),
      **ti_dict,
      'instance_id': ''
    }
    try:
      self.db['tasks'].update_one({'_id': t_hash}, {'$set': t_data}, upsert=True)
      return True
    except Exception as err:
      log.warning('Failed addTaskToDb')
      print(err)
      print(traceback.format_exc())
      return False
    
  def updateTaskInDb(self, t_hash, status, instance_id=None, update_run_time=False):
    """
    Updates the status and instance ID of a task in the database.

    Args:
    - t_hash (str): The hash of the task to update.
    - status (TaskStatus): The new status of the task.
    - instance_id (str, optional): The ID of the instance that is processing the task.
    - update_run_time (bool, optional): Whether to update the first run time of the task.

    Returns:
    - bool: True if the task was updated successfully, False otherwise.
    """
    set_dict = {'status': status}
    if instance_id is not None:
      set_dict['instance_id'] = instance_id
    try:
      if update_run_time:
        start_time = self.db['tasks'].find_one({'_id': t_hash})['timestamp'].astimezone(LOCAL_TZ)
        first_run_end_time = datetime.now().astimezone(LOCAL_TZ)
        first_run_time_taken_ms = (first_run_end_time - start_time).microseconds
        set_dict['first_run_end_time'] = first_run_end_time
        set_dict['first_run_time_taken_ms'] = first_run_time_taken_ms
        
      self.db['tasks'].update_one({'_id': t_hash}, {'$set': set_dict})
      return True
    except Exception as err:
      log.warning('Failed updateTaskInDb')
      print(err)
      print(traceback.format_exc())
      return False
    
  def getTaskFromDb(self, t_hash):
    """
    Retrieves a task from the database given its hash.

    Args:
    - t_hash (str): The hash of the task to retrieve.

    Returns:
    - dict: A dictionary representing the task if it exists in the database, None otherwise.
    """
    try:
      task = self.db['tasks'].find_one({'_id': t_hash})
      return task
    except Exception as err:
      log.warning('Failed getTaskFromDb')
      print(err)
      print(traceback.format_exc())
      return None
    
  ######## WORKER DB FUNCTIONS ########
  
  def addWorkerInDB(self, worker_info, status=WorkerStatus.INITIATED):
    """
    Adds a worker to the database.

    Args:
    - worker_info (dict): A dictionary containing information about the worker to add.
    - status (WorkerStatus): The status of the worker being added. Defaults to WorkerStatus.INITIATED.

    Returns:
    - bool: True if the worker was added successfully, False otherwise.
    """
    self.lock.acquire()
    instance_id = worker_info['instance_id']
    worker_data = {
      **worker_info,
      'status': status,
      'current_task_hash': None,
      'current_task_start_timestamp': None,
      'delete_worker_timestamp': None,
      'add_worker_timestamp': datetime.now().astimezone(LOCAL_TZ),
      # 'last_heartbeat_epoch': int(time.time()),
    }
    self._worker_cache[instance_id] = self.manager.dict(worker_data)
    self.db['workers'].update_one({'_id': instance_id}, {'$set': worker_data}, upsert=True)
    self.lock.release()
    return True
    
  def updateWorkerPorts(self, instance_id, listen_port, health_port):
    """
    Updates the listen and health ports of a worker in the database.

    Args:
    - instance_id (str): The ID of the worker to update.
    - listen_port (int): The new listen port of the worker.
    - health_port (int): The new health port of the worker.

    Returns:
    - bool: True if the worker's ports were updated successfully, False otherwise.
    """
    self.lock.acquire()
    if instance_id not in self._worker_cache:
      self._worker_cache[instance_id] = self.manager.dict(self.getWorkerFromDB(instance_id))
    self._worker_cache[instance_id]['port'] = listen_port
    self._worker_cache[instance_id]['health_port'] = health_port
    self.db['workers'].update_one({'_id': instance_id}, {'$set': {
      'port': listen_port,
      'health_port': health_port
    }})
    self.lock.release()
    return True
    
  def updateWorkerInDB(self, instance_id, status, task_hash=None):
    """
    Update the status and current task hash of a worker in the database.

    Args:
    - instance_id (str): The ID of the worker to update.
    - status (WorkerStatus): The new status of the worker.
    - task_hash (str): The hash of the task the worker is currently working on. Defaults to None.

    Returns:
    - bool: True if the worker was updated successfully, False otherwise.
    """
    self.lock.acquire()
    if instance_id not in self._worker_cache:
      self._worker_cache[instance_id] = self.manager.dict(self.getWorkerFromDB(instance_id))
    update_data = {
      'status': status
    }
    if task_hash is not None:
      update_data['current_task_hash'] = task_hash
      self._worker_cache[instance_id]['current_task_hash'] = task_hash
    if status == WorkerStatus.FREE:
      update_data['mark_free_timestamp'] = datetime.now().astimezone(LOCAL_TZ)
      self._worker_cache[instance_id]['mark_free_timestamp'] = datetime.now().astimezone(LOCAL_TZ)
    self._worker_cache[instance_id]['status'] = status
    self.db['workers'].update_one({'_id': instance_id}, {'$set': update_data})
    self.lock.release()
    return True

  def getWorkerFromDB(self, instance_id):
    """
    Retrieve a worker's information from the database.

    Args:
    - instance_id (str): The ID of the worker to retrieve.

    Returns:
    - dict: A dictionary containing the worker's information.
    """
    if instance_id in self._worker_cache:
      return self._worker_cache[instance_id]
    worker = self.db['workers'].find_one({'_id': instance_id})
    if worker is not None:
      return worker
    else:
      raise Exception('no such worker: ' + instance_id)
    
  def getExactMachineTypeWorkersFromDB(self, exact_machine_type, status=None):
    """
    Retrieve a list of workers with the exact GCE machine type specified.

    Args:
    - exact_machine_type (str): The exact GCE machine type to match.
    - status (WorkerStatus): The status of the workers to match. Defaults to None.

    Returns:
    - list: A list of dictionaries containing the matching workers' information.
    """
    self.lock.acquire()
    matching_workers = []
    for worker in self._worker_cache.values():
      if exact_machine_type == worker['gce_machine_type']:
        if status is None or worker['status'] == status:
          if worker['status'] not in [WorkerStatus.DELETED, WorkerStatus.STALE]:
            matching_workers.append(worker)
    if len(matching_workers)>0:
      self.lock.release()
      return matching_workers
    else:
      workers = self.db['workers'].find({'gce_machine_type': exact_machine_type})
      for worker in workers:
        if status is None or worker['status'] == status:
          if worker['status'] not in [WorkerStatus.DELETED, WorkerStatus.STALE]:
            matching_workers.append(worker)
      self.lock.release()
      return matching_workers
    
  def getMatchingWorkersFromDB(self, cpu, memory, gpu, disk_size, affinity, status=None):
    """
    Retrieve a list of workers that match the specified CPU, memory, GPU, disk size, affinity, and status.

    Args:
    - cpu (int): The minimum required CPU.
    - memory (str): The minimum required memory in GB.
    - gpu (int): The minimum required GPU.
    - disk_size (str): The minimum required disk size in GB.
    - affinity (str): The affinity of the workers to match.
    - status (WorkerStatus): The status of the workers to match. Defaults to None.

    Returns:
    - list: A list of dictionaries containing the matching workers' information.
    """
    self.lock.acquire()
    matching_workers = []
    for worker in self._worker_cache.values():
      if int(worker['cpu']) >= int(cpu) and \
        int(worker['memory'].replace('G', '')) >= int(memory.replace('G', '')) and \
        int(worker['gpu']) >= int(gpu) and \
        int(worker['disk_size'].replace('G', '')) >= int(disk_size.replace('G', '')) and \
        worker['affinity'] == affinity:
        if status is None or worker['status'] == status:
          if worker['status'] not in [WorkerStatus.DELETED, WorkerStatus.STALE]:
            matching_workers.append(worker)
    if len(matching_workers)>0:
      self.lock.release()
      return matching_workers
    else:
      workers = self.db['workers'].find({'affinity': affinity})
      for worker in workers:
        if int(worker['cpu']) >= int(cpu) and \
          int(worker['memory'].replace('G', '')) >= int(memory.replace('G', '')) and \
          int(worker['gpu']) >= int(gpu) and \
          int(worker['disk_size'].replace('G', '')) >= int(disk_size.replace('G', '')) and \
          worker['affinity'] == affinity:
          if status is None or worker['status'] == status:
            if worker['status'] not in [WorkerStatus.DELETED, WorkerStatus.STALE]:
              matching_workers.append(worker)
      self.lock.release()
      return matching_workers
    
  def getAllWorkersFromDB(self, status=[]):
    """
    Retrieve a list of all workers from the database that match the specified status.

    Args:
    - status (list): A list of WorkerStatus values to match. Defaults to an empty list, which returns all workers.

    Returns:
    - list: A list of dictionaries containing the matching workers' information.
    """
    self.lock.acquire()
    workers = []
    for worker in self._worker_cache.values():
      if len(status)==0 or (worker is not None and worker['status'] in status):
        workers.append(worker)
    if len(workers)>0:
      self.lock.release()
      return workers
    else:
      db_workers = self.db['workers'].find({})
      for worker in db_workers:
        if len(status)==0 or worker['status'] in status:
          workers.append(worker)
      self.lock.release()
      return workers
    
  def deleteWorkerFromDB(self, instance_id):
    """
    Delete a worker from the database.

    Args:
    - instance_id (str): The ID of the worker instance to delete.

    Returns:
    - bool: True if the worker was successfully deleted, False otherwise.
    """
    self.lock.acquire()
    try:
      if instance_id in self._worker_cache:
        del self._worker_cache[instance_id]
      self.db['workers'].delete_one({'_id': instance_id})
      self.lock.release()
      return True
    except Exception as err:
      self.lock.release()
      log.warning('Failed deleteWorkerInDB')
      print(err)
      print(traceback.format_exc())
      return False

  ######## APP DB FUNCTIONS ########
  
  def pushNotification(self, level, title, info):
    """
    Pushes a notification to the database.

    Args:
    - level (str): The level of the notification (e.g. 'info', 'warning', 'error').
    - title (str): The title of the notification.
    - info (str): The information contained in the notification.

    Returns:
    - None
    """
    notification = {
      'title': title,
      'info': info,
      'level': level,
      'active': 1
    }
    n_hash = hashlib.md5(yaml.dump(notification).encode('utf-8')).hexdigest()
    self.db['notifications'].update_one({'_id': n_hash}, {'$set': {
      **notification,
      'timestamp': datetime.now().astimezone(LOCAL_TZ)
    }}, upsert=True)
    
# export functions
get_common_db = Database.get_common_db
