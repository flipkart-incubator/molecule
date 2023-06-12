import yaml
import requests
import socket
import os
import subprocess
import shutil
import math
from time import sleep

def setEnvVars(config_file):
  """
  This function reads a YAML configuration file and sets environment variables based on the key-value pairs in the file.

  Args:
  - config_file (str): The path to the YAML configuration file.

  Returns:
  - None
  """
  with open(config_file) as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
    
  for k, v in config.items():
    os.environ[str(k)] = str(v)

def getDetailsFromUI(log):
  """
  This function sends a GET request to a URL specified in an environment variable and returns the response as a JSON object.

  Args:
  - log (logging.Logger): A logger object to log messages.

  Returns:
  - dict: A JSON object containing the response from the URL.
  """
  full_url  = os.environ['MOLECULE_UI_URL'] + 'api/resources/'
  connected = False
  retries = 0
  response = None
  while not connected and retries<3:
    response = requests.get(full_url)
    if response.status_code == 200:
      connected = True
      print(response)
      response = response.json()
    else:
      log.info("Status Code = " + str(response.status_code))
      retries += 1
      if retries < 3:
        log.warning("Failing to communicate to UI. Retrying.")
        log.info("Waiting before trying.")
        sleep(20)
        continue
      else:
        log.error("Failing to communicate to UI. Exiting after retries = " + str(retries))
        raise Exception("Status Code <> 200")
  return response


class TaskStatus:
  """
  This class defines constants for the status of a task.
  """
  QUEUED = 0
  PROCESSING = 1
  COMPLETED = 2
  TERMINATED = 3
  STALE = 4
  

class WorkerStatus:
  """
  This class defines constants for the status of a worker.
  """
  INITIATED = 0
  CREATED = 1
  FREE = 2
  BUSY = 3
  STALE = 4
  DELETED = 5


class InstanceMetadata:
  """
  This class provides methods to get metadata of the instance on which the code is running.
  """
  def __init__(self):
    self.cloud_platform = self.detectCloudPlatform()
    if self.cloud_platform == 'gcp':
      self.metadata_uri = 'http://metadata.google.internal/computeMetadata/v1/instance/'
      self.headers = {'Metadata-Flavor' : 'Google'}
  
  def detectCloudPlatform(self):
    """
    This method detects the cloud platform on which the instance is running.

    Args:
    - None

    Returns:
    - str: The name of the cloud platform.
    """
    try:
      requests.get('http://metadata.google.internal', timeout=3)
      return 'gcp'
    except:
      return 'local'
    
  def getInstanceId(self):
    """
    This method returns the ID of the instance.

    Args:
    - None

    Returns:
    - str: The ID of the instance.
    """
    if self.cloud_platform == 'gcp':
      return str(requests.get(self.metadata_uri + 'id', headers = self.headers).text)
    return str(os.getpid())
  
  def getInstanceName(self):
    """
    This method returns the name of the instance.

    Args:
    - None

    Returns:
    - str: The name of the instance.
    """
    if self.cloud_platform == 'gcp':
      return str(requests.get(self.metadata_uri + 'name', headers = self.headers).text)
    return str(subprocess.check_output(['hostname']).decode('utf-8').strip())
  
  def getInstanceIp(self):
    """
    This method returns the IP address of the instance.

    Args:
    - None

    Returns:
    - str: The IP address of the instance.
    """
    if self.cloud_platform == 'gcp':
      return str(requests.get(self.metadata_uri + 'network-interfaces/0/ip', headers = self.headers).text)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
      s.connect(('10.255.255.255', 1))
      ip = s.getsockname()[0]
    except Exception:
      ip = 'localhost'
    finally:
      s.close()
    return ip
    
  def getInstanceZone(self):
    """
    This method returns the zone of the instance.

    Args:
    - None

    Returns:
    - str: The zone of the instance.
    """
    if self.cloud_platform == 'gcp':
      return str(requests.get(self.metadata_uri + 'zone', headers = self.headers).text.split('/')[-1])
    return str(subprocess.check_output(['hostname', '-f']).decode('utf-8').strip().split('.')[1])
  
  def getMachineType(self):
    """
    This method returns the machine type of the instance.

    Args:
    - None

    Returns:
    - str: The machine type of the instance.
    """
    if self.cloud_platform == 'gcp':
      return str(requests.get(self.metadata_uri + 'machine-type', headers = self.headers).text.split('/')[-1])
    return str(subprocess.check_output(['hostname', '-f']).decode('utf-8').strip().split('.')[0])
  
  def getDiskSize(self):
    """
    This method returns the disk size of the instance.

    Args:
    - None

    Returns:
    - str: The disk size of the instance in GB.
    """
    return str(math.ceil(shutil.disk_usage('/').total / (2**30))) + 'G'
  
  def getCpuCount(self):
    """
    This method returns the number of CPUs of the instance.

    Args:
    - None

    Returns:
    - str: The number of CPUs of the instance.
    """
    return str(os.cpu_count())
  
  def getMemory(self):
    """
    This method returns the memory of the instance.

    Args:
    - None

    Returns:
    - str: The memory of the instance in GB.
    """
    return str(math.ceil(os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (2**30))) + 'G'
  
  def getGpuCount(self):
    """
    This method returns the number of GPUs of the instance.

    Args:
    - None

    Returns:
    - int: The number of GPUs of the instance.
    """
    try:
      return len(subprocess.check_output(['nvidia-smi', '-L']).decode('utf-8').strip().split('\n'))
    except:
      return 0
  
  def getGpuType(self):
    """
    This method returns the type of GPU of the instance.

    Args:
    - None

    Returns:
    - str: The type of GPU of the instance.
    """
    try:
      return subprocess.check_output(['nvidia-smi', 
                                      '--query-gpu=gpu_name', 
                                      '--format=csv']).decode('utf-8').strip().split('\n')[1]
    except:
      return None
    
  def getInstanceLabel(self, label_name):
    """
    This method returns the value of a label of the instance.

    Args:
    - label_name (str): The name of the label.

    Returns:
    - str: The value of the label.
    """
    if self.cloud_platform == 'gcp':
      cmd = "gcloud compute instances describe {} --zone {} --format=\"yaml(labels.{})\"".format(self.getInstanceName(), self.getInstanceZone(), label_name)
      p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
      try:
        return p.stdout.decode('utf-8').strip().split(': ')[1]
      except:
        return None
    return None

  def getGitCommitId(self):
    """
    This method returns the Git commit ID of the code.

    Args:
    - None

    Returns:
    - str: The Git commit ID of the code.
    """
    if self.cloud_platform == 'gcp':
      r = requests.get(self.metadata_uri + 'attributes/git_commit_id', headers = self.headers)
      if r.status_code == 200:
        return str(r.text)
    try:
      return str(subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('utf-8').strip())
    except:
      return None
  
  def getGitTag(self):
    """
    This method returns the Git tag of the code.

    Args:
    - None

    Returns:
    - str: The Git tag of the code.
    """
    if self.cloud_platform == 'gcp':
      r = requests.get(self.metadata_uri + 'attributes/git_tag', headers = self.headers)
      if r.status_code == 200:
        return str(r.text)
    try:
      return str(subprocess.check_output(['git', 'describe', '--tags']).decode('utf-8').strip())
    except:
      return None
