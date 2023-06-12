import os
import yaml
import hashlib
import subprocess
import fcntl
import copy
import getpass
import random
try:
  from google.cloud import storage
except ImportError:
  pass

from tempfile import NamedTemporaryFile

from .gsutil_helper import gsutil_helper

yaml.Dumper.ignore_aliases = lambda *args : True

class Singleton(type):
  _instances = {}

  def __call__(cls, *args, **kwargs):
    if cls not in cls._instances:
      cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
    return cls._instances[cls]


class PipelineStorage(metaclass=Singleton):
  """
  A singleton class that provides methods for storing and retrieving pipeline plans and git repositories.

  Attributes:
    store_loc (str): The location where pipeline plans and git repositories are stored.
  """
  def __init__(self, store_loc):
    self.store_loc = store_loc
    if not os.path.exists(self.store_loc):
      os.makedirs(self.store_loc)

    self.pl_plan_store = os.path.join(self.store_loc, 'pipeline_plans')
    if not os.path.exists(self.pl_plan_store):
      os.makedirs(self.pl_plan_store)

    self.git_store = os.path.join(self.store_loc, 'git')
    if not os.path.exists(self.git_store):
      os.makedirs(self.git_store)

  @staticmethod
  def computePipelineHash(task_dict):
    """
    Computes the MD5 hash of a pipeline plan dictionary, excluding the task group list and replacing the task group dictionary with a dictionary of task group hashes.

    Args:
      task_dict (dict): A dictionary representing the pipeline plan.

    Returns:
      str: The MD5 hash of the pipeline plan dictionary.
    """
    hash_dict = copy.deepcopy(task_dict)
    if type(hash_dict) != dict:
      hash_dict = hash_dict.serializeDict()
    hash_dict['tg_list'] = []
    for task_hash, ti in hash_dict['tg_dict'].items():
      hash_dict['tg_dict'][task_hash] = ti['hashes']
    return hashlib.md5(yaml.dump(hash_dict).encode('utf-8')).hexdigest()

  @staticmethod
  def runCommand(command, lockfile):
    """
    Runs a shell command and returns True if the command was successful, False otherwise.

    Args:
      command (str): The shell command to run.
      lockfile (str): A unique identifier for the lockfile.

    Returns:
      bool: True if the command was successful, False otherwise.
    """
    f = open('/tmp/' + lockfile + str(random.randint(10000000, 99999999)), 'w')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

    p = subprocess.run(command, shell=True)

    fcntl.lockf(f, fcntl.LOCK_UN)
    if p.returncode == 0:
      return True
    else:
      return False

  @staticmethod
  def saveConfig(c_dict, c_name):
    """
    Saves a configuration dictionary to a YAML file.

    Args:
      c_dict (dict): A dictionary representing the configuration to be saved.
      c_name (str): The name of the configuration.

    Returns:
      str: The path to the saved YAML file.
    """
    loc = '/tmp/' + c_name + str(random.randint(10000000, 99999999)) + '.yaml'
    with open(loc, 'w') as f:
      yaml.dump(c_dict, f)
    return loc

  def gitClone(self, url, commit_id):
    """
    Clones a git repository at the specified URL and checks out the specified commit ID.

    Args:
      url (str): The URL of the git repository to clone.
      commit_id (str): The commit ID to check out.

    Returns:
      str: The path to the cloned git repository.

    Raises:
      Exception: If the git clone or checkout fails.
    """
    clone_path = os.path.join(self.git_store, commit_id)
    if not os.path.exists(clone_path):
      os.makedirs(clone_path)

    if not os.listdir(clone_path):
      cmd = 'git clone -n {url} {path}'.format(
        url=url,
        path=clone_path
      )
      if self.runCommand(cmd, commit_id) is False:
        cmd = 'rm -rf {path}'.format(path=clone_path)
        self.runCommand(cmd, commit_id)
        return False

    cmd = 'cd {path} && git checkout {commit}'.format(
      path=clone_path,
      commit=commit_id
    )
    if self.runCommand(cmd, commit_id) is False:
      return False

    return clone_path

  def savePipeline(self, task_dict):
    """
    Saves the pipeline plan specified by the given task dictionary.

    Args:
      task_dict (dict): The task dictionary representing the pipeline plan.
    """
    pl_hash = self.computePipelineHash(task_dict)
    # pipeline_dict = yaml.load(
    #     yaml.dump(task_dict), Loader=yaml.BaseLoader)
    # doc_ref = self.db.collection('pipelines').document(document_id=pl_hash)
    # doc_ref.set(pipeline_dict)
    loc = os.path.join(self.pl_plan_store, pl_hash + '.yaml')
    with open(loc, 'w') as exec_spec:
      yaml.dump(task_dict, exec_spec)
    os.chmod(loc, 0o777)

  def loadPipeline(self, pl_hash):
    """
    Loads the pipeline plan specified by the given pipeline hash.

    Args:
      pl_hash (str): The hash of the pipeline plan to load.

    Returns:
      dict: The task dictionary representing the loaded pipeline plan.
    """
    # doc = self.db.collection('pipelines').document(pl_hash).get()
    # if doc.exists:
    #   return doc.to_dict()
    # raise Exception(pl_hash + " does not exists in transforms")
    loc = os.path.join(self.pl_plan_store, pl_hash + '.yaml')
    with open(loc, 'r') as exec_spec:
      pl_spec = yaml.load(exec_spec, Loader=yaml.Loader)
    return pl_spec

class SpawnerStorage:
  """
  A class that provides methods for storing and retrieving spawner configurations.

  Attributes:
    store_loc (str): The location where spawner configurations are stored.
  """
  def __init__(self, local_store_loc, remote_ip=None, remote_store_loc=None, mode='nfs', db=None):
    self.mode = mode
    self.db = db
    self.local_data_store = os.path.join(local_store_loc, 'data')
    if self.mode == 'nfs' and not os.path.exists(self.local_data_store):
      os.makedirs(self.local_data_store)
    self.remote_ip = remote_ip
    if remote_store_loc is not None:
      self.remote_data_store = os.path.join(remote_store_loc, 'data')
    else:
      self.remote_data_store = None

  def pullCode(self, code_loc):
    """
    Downloads the code from the specified location and extracts it to a local directory.

    Args:
      code_loc (str): The location of the code to download.

    Returns:
      str: The local directory where the code was extracted.
    """
    if self.mode == 'gcs':
      blob_loc = code_loc.split('bucket/')[1]
    else:
      blob_loc = code_loc
    blob_name = blob_loc.split('/')[-1].split('.')[0]
    if os.path.exists('/mnt/data'):
      local_code_loc = f'/mnt/data/molecule/dev/{blob_name}/'
    else:
      local_code_loc = f'/tmp/molecule/dev/{blob_name}/'
    os.makedirs(local_code_loc, exist_ok=True)
    
    if self.mode == 'gcs':
      self.bucket = storage.Client(
          project=os.environ['GOOGLE_CLOUD_PROJECT']).bucket(os.environ['MOLECULE_BUCKET_NAME'])
      blob = self.bucket.blob(blob_loc)

      with NamedTemporaryFile('wb', suffix='.tar') as f:
        blob.download_to_filename(f.name)
        subprocess.run(f'tar -xf {f.name} -C {local_code_loc}', shell=True)
    else:
      subprocess.run(f'tar -xf {blob_loc} -C {local_code_loc}', shell=True)
    
    return os.path.join(local_code_loc, 'code')

  ## Deprecated in gcs 
  def localCheck(self, hash_key):
    """
    Checks if the specified hash key exists in the local data store and is not locked for writing.

    Args:
      hash_key (str): The hash key to check.

    Returns:
      bool: True if the hash key exists in the local data store and is not locked for writing, False otherwise.
    """
    loc = os.path.join(self.local_data_store, hash_key)
    write_lock = os.path.join(loc, 'write.lock')
    if self.mode == 'nfs':
      return (os.path.exists(loc) and not os.path.exists(write_lock))
    if self.mode == 'gcs':
      if os.path.exists(loc) and not os.path.exists(write_lock):
        return True
      else:
        return gsutil_helper(loc, mode='check') and not gsutil_helper(write_lock, mode='check')

  def check(self, hash_key):
    """
    Checks if the specified hash key exists in the database.

    Args:
      hash_key (str): The hash key to check.

    Returns:
      bool: True if the hash key exists in the database, False otherwise.
    """
    if self.db.checkDatasetHash(hash_key):
      return True
    return False

  ## Deprecated in gcs  
  def sync(self, hash_key):
    """
    Syncs the specified hash key from the remote data store to the local data store.

    Args:
      hash_key (str): The hash key to sync.

    Returns:
      bool: True if the hash key was successfully synced, False otherwise.

    Raises:
      Exception: If the hash key does not exist in the store, or if there was an error syncing the hash key.
    """
    if self.localCheck(hash_key) is True:
      return True
    if not self.check(hash_key):
      raise Exception('No such hash in store: ', hash_key)

    remote_loc = os.path.join(self.remote_data_store, hash_key)
    f = open('/tmp/' + hash_key, 'w')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

    if self.remote_ip is not None:
      cmd = 'rsync -e "ssh -o ConnectTimeout=5" --info=progress2 -rzh ' + self.remote_ip + ':' + remote_loc + ' ' + self.local_data_store
      p = subprocess.run(cmd, shell=True)

      fcntl.lockf(f, fcntl.LOCK_UN)
      
      if p.returncode == 0:
        return True
      else:
        raise Exception('SCP file transfer error', hash_key)
    elif self.remote_ip is None and self.remote_data_store is not None:
      cmd = 'cp -r ' + remote_loc + ' ' + self.local_data_store
      p = subprocess.run(cmd, shell=True)
      
      fcntl.lockf(f, fcntl.LOCK_UN)
      
      synced_path = os.path.join(self.local_data_store, hash_key)
      # 7 days TTL for copied data
      ttl_file = os.path.join(synced_path, 'del_7.ttl')
      
      if p.returncode == 0 and os.path.exists(synced_path):
        open(ttl_file, 'a').close()
        return True
      else:
        raise Exception('Data Copy failure', hash_key)
    else:
      fcntl.lockf(f, fcntl.LOCK_UN)
      raise Exception('Data Sync failure', hash_key)
