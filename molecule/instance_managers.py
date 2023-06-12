import os
import random
import subprocess
import tempfile
import pandas as pd

from .resources import InstanceMetadata
from . import pl_logger as logger

log = logger.getLogger('instance_manager', enable_cloud_logging=False)


class GCPInstanceManager:
  """
  A class to manage Google Cloud Platform (GCP) instances.

  Attributes:
  - gcp_project (str): The name of the GCP project.
  - utils_bucket_name (str): The name of the GCP bucket containing utility scripts.
  - deployment_type (str): The type of deployment (e.g. stage, prod).
  - git_commit_id (str): The ID of the Git commit.
  - git_tag (str): The Git tag associated with the commit.
  - machine_df (pandas.DataFrame): A dataframe containing information about available machine types.

  Methods:
  - listMachineTypesbyZones(zones): Lists machine types available in the specified zones.
  - getMachineTypeZoneInfo(worker_type): Returns the machine type and zone information for the specified worker type.
  - getInstanceIP(instance_name, zone): Returns the IP address of the specified instance.
  - getInstanceID(instance_name, zone): Returns the ID of the specified instance.
  - createInstance(instance_name, zone, machine_type, disk_size_in_gb, affinity, provision_model): Creates a new instance with the specified parameters.
  - deleteInstance(instance_name, zone): Deletes the specified instance.
  """

  def __init__(self):
    """
    Initializes a new GCPInstanceManager object.

    Args:
    - gcp_project (str): The name of the GCP project.
    - utils_bucket_name (str): The name of the GCP bucket containing utility scripts.
    """
    self.gcp_project = os.environ['GOOGLE_CLOUD_PROJECT']
    self.gcp_service_account = os.environ['GOOGLE_SERVICE_ACCOUNT']
    self.utils_bucket_name = os.environ['UTILS_BUCKET_NAME']
    self.cpu_startup_script = "gs://{utils_bucket_name}/gcp_spawner.sh".format(utils_bucket_name=self.utils_bucket_name)
    # self.hive_startup_script = "gs://{utils_bucket_name}/hive_spawner.sh".format(utils_bucket_name=self.utils_bucket_name)
    self.base_create_cmd = "gcloud compute instances create {instance_name} --project=" + self.gcp_project + " --zone={zone} --machine-type={machine_type} --metadata=shutdown-script=pkill\\ -2\\ python3\\ \\&\\&\\ sleep\\ 2\\ \\&\\&\\ pkill\\ -2\\ python3\\ \\&\\&\\ sleep\\ 2\\ \\&\\&\\ pkill\\ -2\\ python3,startup-script-url={startup_script},enable-oslogin=true,git_commit_id={git_commit_id},git_tag={git_tag} --no-restart-on-failure --maintenance-policy=TERMINATE --provisioning-model={provision_model} --instance-termination-action=DELETE --service-account=" + self.gcp_service_account + " --scopes=https://www.googleapis.com/auth/cloud-platform --create-disk=auto-delete=yes,boot=yes,device-name={instance_name},image-family=molecule-{affinity}-spawner-family,mode=rw,size={disk_size_in_gb},type=projects/" + self.gcp_project + "/zones/asia-south1-c/diskTypes/pd-balanced --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --reservation-affinity=any --labels=\"molecule_platform=true,component=spawner,deployment_type={deployment_type},utils_bucket_name={utils_bucket_name}\""
    self.base_delete_cmd = "gcloud compute instances delete {instance_name} --project=" + self.gcp_project + " --zone={zone} --quiet"
    self.im = InstanceMetadata()
    self.deployment_type = self.im.getInstanceLabel('deployment_type')
    if self.deployment_type is None:
      self.deployment_type = 'stage'
    self.git_commit_id = self.im.getGitCommitId()
    self.git_tag = self.im.getGitTag()
    self.machine_df = None

  def listMachineTypesbyZones(self, zones):
    """
    Lists machine types available in the specified zones.

    Args:
    - zones (str): A comma-separated list of zones to search for machine types.

    Returns:
    - pandas.DataFrame: A dataframe containing information about available machine types.
    """
    if self.machine_df is None:
      f = tempfile.NamedTemporaryFile(suffix='.csv')
      cmd = str("gcloud compute machine-types list --project=" + self.gcp_project + " --zones={} --format=\"csv(name,zone,guestCpus,memoryMb)\" > {}").format(zones, f.name)
      p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      if p.returncode != 0:
        log.error("Error listing machine types: {}".format(p.stderr))
        return []
      self.machine_df = pd.read_csv(f.name)
      # add machine_family to machine_df
      self.machine_df['machine_family'] = self.machine_df['name'].apply(lambda x: x.split('-')[0])
    return self.machine_df

  def getMachineTypeZoneInfo(self, worker_type):
    """
    Returns the machine type and zone information for the specified worker type.

    Args:
    - worker_type (dict): A dictionary containing information about the worker type.

    Returns:
    - tuple: A tuple containing the machine type, zone, CPU count, and memory.
    """
    if worker_type['affinity'] == 'hive':
      return 'e2-standard-2', 'asia-south1-c', 2, '8G'
    else:
      machines_df = self.listMachineTypesbyZones('asia-south1-a,asia-south1-b,asia-south1-c')
      # filter machines by requirements
      machines_df = machines_df[machines_df['cpus'] >= int(worker_type['cpu'])]
      machines_df = machines_df[machines_df['memory_gb'] >= int(worker_type['memory'].replace('G', ''))]
      if worker_type['affinity'] == 'cpu':
        # if machine_family e2 is available, use it else use n1
        if 'e2' in machines_df['machine_family'].unique():
          machine_family = 'e2'
        else:
          machine_family = 'n1'
        # filter machines by machine_family
        machines_df = machines_df[machines_df['machine_family'] == machine_family]
        # sort by cpu count and then memory
        machines_df = machines_df.sort_values(by=['cpus', 'memory_gb'])
        # return the first machine name, zone, cpu, memory
        return str(machines_df['name'].iloc[0]), str(machines_df['zone'].iloc[0]), int(machines_df['cpus'].iloc[0]), str(int(machines_df['memory_gb'].iloc[0])) + 'G'
      elif worker_type['affinity'] == 'gpu':
        machine_family = 'n1'
        # TODO: return correct machine for GPU
        return 'n1-standard-16', 'asia-south1-a', 16, '60G'
      else:
        log.critical("Invalid affinity type: {}".format(worker_type['affinity']))
        return 'e2-standard-2', 'asia-south1-a', 2, '8G'

  def getInstanceIP(self, instance_name, zone):
    """
    Returns the IP address of the specified instance.

    Args:
    - instance_name (str): The name of the instance.
    - zone (str): The zone in which the instance is located.

    Returns:
    - str: The IP address of the instance.
    """
    cmd = str("gcloud compute instances describe {} --zone {} --project=" + self.gcp_project + " --format=\"value(networkInterfaces[0].networkIP)\"").format(instance_name, zone)
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode != 0:
      log.error("Error getting instance IP: {}".format(p.stderr))
      return None
    return p.stdout.decode('utf-8').strip()

  def getInstanceID(self, instance_name, zone):
    """
    Returns the ID of the specified instance.

    Args:
    - instance_name (str): The name of the instance.
    - zone (str): The zone in which the instance is located.

    Returns:
    - str: The ID of the instance.
    """
    cmd = str("gcloud compute instances describe {} --zone {} --project=" + self.gcp_project + " --format=\"value(id)\"").format(instance_name, zone)
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode != 0:
      log.error("Error getting instance ID: {}".format(p.stderr))
      return None
    return p.stdout.decode('utf-8').strip()

  def createInstance(self, instance_name, zone, machine_type, disk_size_in_gb, affinity, provision_model):
    """
    Creates a new instance with the specified parameters.

    Args:
    - instance_name (str): The name of the new instance.
    - zone (str): The zone in which to create the instance.
    - machine_type (str): The machine type to use for the instance.
    - disk_size_in_gb (int): The size of the boot disk in GB.
    - affinity (str): The affinity type of the instance (e.g. cpu, gpu, hive).
    - provision_model (str): The provision model to use for the instance.

    Returns:
    - bool: True if the instance was created successfully, False otherwise.
    """
    if affinity == 'cpu':
      startup_script = self.cpu_startup_script
    elif affinity == 'hive':
      startup_script = self.hive_startup_script
    cmd = self.base_create_cmd.format(instance_name=instance_name, zone=zone, machine_type=machine_type, disk_size_in_gb=disk_size_in_gb,
                                      startup_script=startup_script, provision_model=provision_model, affinity=affinity, 
                                      deployment_type=self.deployment_type, utils_bucket_name=self.utils_bucket_name, 
                                      git_commit_id=self.git_commit_id, git_tag=self.git_tag)
    log.info("Creating instance: {}".format(cmd))
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.info("Instance creation output: {}".format(p.stdout.decode('utf-8')))
    log.info("Instance creation error: {}".format(p.stderr.decode('utf-8')))
    if 'INTERNAL_IP' in p.stdout.decode('utf-8'):
      return True
    else:
      return False

  def deleteInstance(self, instance_name, zone):
    """
    Deletes the specified instance.

    Args:
    - instance_name (str): The name of the instance to delete.
    - zone (str): The zone in which the instance is located.

    Returns:
    - int: The return code of the gcloud command.
    """
    cmd = self.base_delete_cmd.format(instance_name=instance_name, zone=zone)
    log.info("Deleting instance: {}".format(cmd))
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.info("Instance deletion output: {}".format(p.stdout))
    log.info("Instance deletion error: {}".format(p.stderr))
    return p.returncode


class LocalInstanceManager:
  """
  A class that provides an interface to manage local instances.
  """
  def __init__(self):
    """
    Initializes a new instance of the LocalInstanceManager class.
    """
    self.deployment_type = 'local'
    self.process_id_map = {}

  def getMachineTypeZoneInfo(self, worker_type):
    """
    Returns the machine type and zone information for the specified worker type.

    Args:
    - worker_type (str): The type of worker for which to retrieve machine type and zone information.

    Returns:
    - tuple: A tuple containing the machine type and zone information for the specified worker type.
    """
    return {'cpu': 4, 'memory': '16G', 'disk_size': '10G', 'gpu': 0, 'affinity': 'cpu', 'docker_image': 'alpine:latest'}, 'local', 4, '16G'

  def getInstanceIP(self, instance_name, zone):
    """
    Returns the IP address of the specified instance.

    Args:
    - instance_name (str): The name of the instance.
    - zone (str): The zone in which the instance is located.

    Returns:
    - str: The IP address of the specified instance.
    """
    return 'localhost'

  def getInstanceID(self, instance_name, zone):
    """
    Returns the ID of the specified instance.

    Args:
    - instance_name (str): The name of the instance.
    - zone (str): The zone in which the instance is located.

    Returns:
    - str: The ID of the specified instance, or the instance name if the ID is not found.
    """
    return self.process_id_map.get(instance_name, instance_name)

  def createInstance(self, instance_name, zone, machine_type, disk_size_in_gb, affinity, provision_model):
    """
    Creates a new instance with the specified parameters.

    Args:
    - instance_name (str): The name of the new instance.
    - zone (str): The zone in which to create the new instance.
    - machine_type (dict): A dictionary containing the machine type information for the new instance.
    - disk_size_in_gb (int): The size of the disk for the new instance, in gigabytes.
    - affinity (str): The affinity of the new instance.
    - provision_model (str): The provision model for the new instance.

    Returns:
    - bool: True if the instance was created successfully, False otherwise.
    """
    # create spawner with name instance_name and affinity
    # check if gpu is requested
    if 'gpu' in machine_type and machine_type['gpu'] > 0:
      spawner_cmd = 'molecule-spawner -o -as -p {port} -s {store_loc} -mt gpu --storage-type nfs'.format(port=random.randint(2000,9999), store_loc=os.environ.get('store_loc'))
    else:
      spawner_cmd = 'molecule-spawner -o -as -p {port} -s {store_loc} -mt cpu --storage-type nfs'.format(port=random.randint(2000,9999), store_loc=os.environ.get('store_loc'))
    print("Trying to create instance: {}".format(spawner_cmd))
    p = subprocess.Popen(spawner_cmd, shell=True)
    self.process_id_map[instance_name] = str(p.pid)
    print("Created instance: {} {}".format(instance_name, p.pid))
    return True

  def deleteInstance(self, instance_name, zone):
    """
    Deletes the specified instance.

    Args:
    - instance_name (str): The name of the instance to delete.
    - zone (str): The zone in which the instance is located.

    Returns:
    - bool: True if the instance was deleted successfully, False otherwise.
    """
    # kill process_id with instance_name
    print("Trying to delete instance: {}".format(instance_name))
    if instance_name in self.process_id_map:
      p = subprocess.run("kill -9 {}".format(self.process_id_map[instance_name]), shell=True)
      if p.returncode == 0:
        return True
    return False


class CloudPlatformInstanceManager:
  """
  A class that provides an interface to manage instances across different cloud platforms.
  """
  def __init__(self, platforms = ['gcp']):
    """
    Initializes the CloudPlatformInstanceManager object.

    Args:
    - platforms (list): A list of cloud platforms to be supported. Default is ['gcp'].

    Returns:
    - None
    """
    self.gcpim = GCPInstanceManager()
    self.localim = LocalInstanceManager()

  def listMachineTypesbyZones(self, zones, platform='gcp'):
    """
    Lists the machine types available in the specified zones.

    Args:
    - zones (list): A list of zones to list the machine types for.
    - platform (str): The cloud platform to list the machine types for. Default is 'gcp'.

    Returns:
    - A pandas dataframe containing the machine types available in the specified zones.
    """
    if platform == 'gcp':
      return self.gcpim.listMachineTypesbyZones(zones)
    else:
      raise Exception('No such platform supported')

  def getMachineTypeZoneInfo(self, worker_type, platform='gcp'):
    """
    Gets the machine type and zone information for the specified worker type.

    Args:
    - worker_type (str): The worker type to get the machine type and zone information for.
    - platform (str): The cloud platform to get the machine type and zone information for. Default is 'gcp'.

    Returns:
    - A tuple containing the machine type and zone information for the specified worker type.
    """
    if platform == 'gcp':
      return self.gcpim.getMachineTypeZoneInfo(worker_type)
    elif platform == 'local':
      return self.localim.getMachineTypeZoneInfo(worker_type)
    else:
      raise Exception('No such platform supported')

  def getInstanceIP(self, instance_name, zone, platform='gcp'):
    """
    Gets the IP address of the specified instance.

    Args:
    - instance_name (str): The name of the instance to get the IP address for.
    - zone (str): The zone the instance is located in.
    - platform (str): The cloud platform to get the IP address for. Default is 'gcp'.

    Returns:
    - The IP address of the specified instance.
    """
    if platform == 'gcp':
      return self.gcpim.getInstanceIP(instance_name, zone)
    elif platform == 'local':
      return self.localim.getInstanceIP(instance_name, zone)
    else:
      raise Exception('No such platform supported')

  def getInstanceID(self, instance_name, zone, platform='gcp'):
    """
    Gets the ID of the specified instance.

    Args:
    - instance_name (str): The name of the instance to get the ID for.
    - zone (str): The zone the instance is located in.
    - platform (str): The cloud platform to get the ID for. Default is 'gcp'.

    Returns:
    - The ID of the specified instance.
    """
    if platform == 'gcp':
      return self.gcpim.getInstanceID(instance_name, zone)
    elif platform == 'local':
      return self.localim.getInstanceID(instance_name, zone)
    else:
      raise Exception('No such platform supported')

  def createInstance(self, instance_name, zone, machine_type, disk_size_in_gb, affinity, provision_model, platform='gcp'):
    """
    Creates a new instance.

    Args:
    - instance_name (str): The name of the instance to create.
    - zone (str): The zone to create the instance in.
    - machine_type (str): The machine type to use for the instance.
    - disk_size_in_gb (int): The size of the disk to use for the instance.
    - affinity (str): The affinity of the instance.
    - provision_model (str): The provision model to use for the instance.
    - platform (str): The cloud platform to create the instance on. Default is 'gcp'.

    Returns:
    - True if the instance was created successfully, False otherwise.
    """
    if platform == 'gcp':
      return self.gcpim.createInstance(instance_name, zone, machine_type, disk_size_in_gb, affinity, provision_model)
    elif platform == 'local':
      return self.localim.createInstance(instance_name, zone, machine_type, disk_size_in_gb, affinity, provision_model)
    else:
      raise Exception('No such platform supported')

  def deleteInstance(self, instance_name, zone, platform='gcp'):
    """
    Deletes the specified instance.

    Args:
    - instance_name (str): The name of the instance to delete.
    - zone (str): The zone the instance is located in.
    - platform (str): The cloud platform to delete the instance from. Default is 'gcp'.

    Returns:
    - The return code of the instance deletion command.
    """
    if platform == 'gcp':
      return self.gcpim.deleteInstance(instance_name, zone)
    elif platform == 'local':
      return self.localim.deleteInstance(instance_name, zone)
    else:
      raise Exception('No such platform supported')
    
  def getDeploymentType(self, platform='gcp'):
    """
    Gets the deployment type for the specified cloud platform.

    Args:
    - platform (str): The cloud platform to get the deployment type for. Default is 'gcp'.

    Returns:
    - The deployment type for the specified cloud platform.
    """
    if platform == 'gcp':
      return self.gcpim.deployment_type
    elif platform == 'local':
      return self.localim.deployment_type
    else:
      raise Exception('No such platform supported')
    
  def getBestPlatform(self, worker_type):
    """
    Gets the best cloud platform for the specified worker type.

    Args:
    - worker_type (str): The worker type to get the best cloud platform for.

    Returns:
    - The best cloud platform for the specified worker type.
    """
    # worker_type = {
    #   'cpu': '8',
    #   'memory': '16G',
    #   'disk_size': '100G',
    #   'gpu': '1',
    #   'affinity': 'cpu',
    # }
    if worker_type['cpu'] > 4 or worker_type['memory'] > '8G' or worker_type['disk_size'] > '50G' or worker_type['gpu'] > 0:
      return 'gcp'
    else:
      return 'local'


