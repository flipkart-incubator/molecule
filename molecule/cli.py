import argparse
import getpass
import yaml
import requests
import os
import sys
import logging

from .resources import setEnvVars
from . import pipelines, transforms, pl_storage
from .spec_parser import SpecParser
from .zmqserver import MessageBase
from . import pl_logger as logger
from .database import Database
from .resolve import Resolve, Metadata
from .autoscaler import AutoScalingPolicy

log = logging.getLogger(__name__)

class CLI:
  """
  A class that provides methods to generate, submit, and kill pipelines.
  """
  def __init__(self, ui_url):
    """
    Initializes the CLI class with the given UI URL.

    Args:
    - ui_url (str): The URL of the UI.
    """
    self.command_url = ui_url + 'api/command'
    self.store = pl_storage.PipelineStorage('/tmp/molecule_{user}'.format(user=getpass.getuser()))

  def generatePipeline(self, pipeline_name, pipeline_spec_path, pipeline_config_path, queue, message='', project='default'):
    """
    Generates a pipeline with the given pipeline name, pipeline spec path, pipeline config path, queue, message, and project.

    Args:
    - pipeline_name (str): The name of the pipeline.
    - pipeline_spec_path (str): The path to the pipeline specification.
    - pipeline_config_path (str): The path to the pipeline configuration.
    - queue (str): The queue to use.
    - message (str): The message to use.
    - project (str): The project to use.

    Returns:
    - data (dict): A dictionary containing the generated pipeline data.
    """
    task_dict, output_hash, pipeline = CLI.getTaskDict(
        pipeline_name, pipeline_spec_path, pipeline_config_path)
    pipeline_hash = self.store.computePipelineHash(task_dict)

    log.info('Processing Pipeline Successful')
    print('**************** PIPELINE INFO ****************')
    print('Pipeline Name: ' + pipeline_name)
    print('Pipeline Hash: ' + pipeline_hash)
    print('Output Hashes:')
    print(yaml.dump(output_hash, indent=2))
    print('***********************************************')

    with open(pipeline_config_path) as file:
      pipeline_config = yaml.load(file, Loader=yaml.FullLoader)
    data = {
        'task_dict': task_dict.serializeDict(),
        'queue': queue,
        'pipeline': pipeline.serializeDict(),
        'name': pipeline_name,
        'hash': pipeline_hash,
        'config': pipeline_config,
        'user': getpass.getuser(),
        'message': message,
        'project': project,
        'enforce_ttl': True
    }

    return data

  def submitPipeline(self, data):
    """
    Submits a pipeline with the given data.

    Args:
    - data (dict): The data of the pipeline to submit.
    """
    post_data = {
      'cmd_str': MessageBase.COMMAND_SUBMIT_GRAPH,
      'cmd_param': data,
      'expected_response': MessageBase.ACK_SUBMIT_GRAPH,
      'version': MessageBase.VERSION
    }
    response = requests.post(self.command_url, json=post_data)
    if response.json()['response'] != MessageBase.ACK_SUBMIT_GRAPH:
      print(response.json()['response'])
      raise Exception('Graph submission failed, please pull the latest code and try again!')

  def killPipeline(self, pipeline_name, pipeline_spec_path, pipeline_config_path, debug=None):
    """
    Kills a pipeline with the given pipeline name, pipeline spec path, pipeline config path, and debug.

    Args:
    - pipeline_name (str): The name of the pipeline.
    - pipeline_spec_path (str): The path to the pipeline specification.
    - pipeline_config_path (str): The path to the pipeline configuration.
    - debug (bool): Whether to enable debug mode.
    """
    task_dict, output_hash, _ = CLI.getTaskDict(pipeline_name, pipeline_spec_path, pipeline_config_path, debug)
    pipeline_hash = self.store.computePipelineHash(task_dict)
    del task_dict
    post_data = {
      'cmd_str': MessageBase.COMMAND_DELETE_GRAPH,
      'cmd_param': pipeline_hash,
      'expected_response': MessageBase.ACK_DELETE_GRAPH,
      'version': MessageBase.VERSION
    }
    response = requests.post(self.command_url, json=post_data)
    if response.json()['response'] != MessageBase.ACK_DELETE_GRAPH:
      raise Exception('Pipeline Kill failed')

  def bugPipeline(self, data):
    """
    Reports a bug with the given data.

    Args:
    - data (dict): The data of the bug to report.
    """
    post_data = {
      'cmd_str': 'BUG',
      'cmd_param': data,
      'expected_response': 'ACK_BUG',
      'version': MessageBase.VERSION
    }
    response = requests.post(self.command_url, json=post_data)
    if response.json()['response'] != 'ACK_BUG':
      log.critical(response.json()['response'])
      raise Exception('Bug submission failed')

  def updateAutoscalePolicy(self):
    """
    Updates the autoscale policy.
    """
    post_data = {
      'cmd_str': MessageBase.COMMAND_REFRESH_AUTOSCALE_POLICY,
      'cmd_param': '',
      'expected_response': MessageBase.ACK_REFRESH,
      'version': MessageBase.VERSION
    }
    response = requests.post(self.command_url, json=post_data)
    if response.json()['response'] != MessageBase.ACK_REFRESH:
      print(response.json()['response'])
      raise Exception('Autoscale policy update failed')

  @staticmethod
  def getTaskDict(pipeline_name, pipeline_spec_path, pipeline_config_path, debug=None):
    """
    Gets the task dictionary, output hash, and pipeline with the given pipeline name, pipeline spec path, pipeline config path, and debug.

    Args:
    - pipeline_name (str): The name of the pipeline.
    - pipeline_spec_path (str): The path to the pipeline specification.
    - pipeline_config_path (str): The path to the pipeline configuration.
    - debug (bool): Whether to enable debug mode.

    Returns:
    - task_dict (dict): The task dictionary.
    - output_hash (dict): The output hash.
    - pipeline (Pipeline): The pipeline.
    """
    with open(pipeline_config_path) as file:
      pipeline_config = yaml.load(file, Loader=yaml.Loader)
    transforms.loadTransforms(debug=debug)
    pipelines.loadPipeline(pipeline_spec_path)
    pl_class = SpecParser.TransformRegistry[pipeline_name]
    pipeline = pl_class()
    task_dict, output_hash = pipeline.getTaskGraph(pipeline_config, None)

    return task_dict, output_hash, pipeline

def main():
  arg_parser = argparse.ArgumentParser(description='utility for managing pipelines services')

  arg_parser.add_argument('-c', '--server-config', action='store', help='Path to server configuration variables')
  arg_parser.add_argument('-sp', '--submit-pipeline', action='store_true', help='Submit pipeline')
  arg_parser.add_argument('-gp', '--generate-pipeline', action='store_true', help='Generate pipeline')
  arg_parser.add_argument('-kp', '--kill-pipeline', action='store_true', help='Kill pipeline')
  arg_parser.add_argument('-pn', '--pipeline-name', action='store', help='Pipeline Name')
  arg_parser.add_argument('-ps', '--pipeline-spec-path', action='store', help='Pipeline Spec Path')
  arg_parser.add_argument('-pc', '--pipeline-config-path', action='store', help='Pipeline Config')
  arg_parser.add_argument('-d', '--debug', action='store', help='debug run')
  arg_parser.add_argument('-ph', '--pipeline-hash', action='store', help='Pipeline Hash')
  arg_parser.add_argument('-q', '--queue', action='store', help='queue: p0,p1,p2')
  arg_parser.add_argument('-m', '--message', action='store', help='Commit Message')
  arg_parser.add_argument('-p', '--project', action='store', help='Project Name')
  arg_parser.add_argument('-uc', '--update-config', action='store_true', help='update config')
  arg_parser.add_argument('-ttl', '--time-to-live', action='store', help='TTL for files in days. Allowed values - 14, 28, 60, 120')
  arg_parser.add_argument('-dd', '--delete-dataset', action='store', help='Delete dataset Hash')
  arg_parser.add_argument('-o', '--offline', action='store_true', help='Update server details when online')
  arg_parser.add_argument('-uasp', '--update-autoscale-policy', action='store_true', help='Update autoscale policy flag')
  arg_parser.add_argument('--env', action='store', help='stage or prod storage')

  # Parsing the arguments
  args = arg_parser.parse_args()

  # Checking if generate_pipeline flag is set
  if args.generate_pipeline:
    # Disabling logging and redirecting stdout to devnull
    logging.disable(logging.CRITICAL)
    sys.stdout = open(os.devnull, 'w')
    if args.debug == 'local':
      args.debug = None
    # Generating pipeline plan
    pipeline_plan, _, _ = CLI.getTaskDict(args.pipeline_name, args.pipeline_spec_path, args.pipeline_config_path, debug=args.debug)
    sys.stdout = sys.__stdout__
    # Printing the pipeline plan in yaml format
    print(yaml.dump(pipeline_plan, indent=4))
    sys.exit(0)

  # Creating a logger
  global log
  log = logger.getLogger('client', enable_cloud_logging=False)

  # Checking if server_config argument is provided
  if args.server_config is None:
    raise Exception('Please provide server config file path')
  else:
    # Setting environment variables and loading server config
    setEnvVars(args.server_config)
    with open(args.server_config, 'r') as fl:
      server_config = yaml.load(fl, Loader=yaml.Loader)

  pipeline_config = None
  if args.pipeline_config_path is not None:
    with open(args.pipeline_config_path) as f:
      pipeline_config = yaml.load(f, Loader=yaml.FullLoader)

  # Setting default queue to p2 if not provided or invalid
  if args.queue is None or args.queue.lower() not in ['p0', 'p1', 'p2']:
    args.queue = 'p2'
  else:
    args.queue = args.queue.lower()

  # Logging the arguments
  log.info('Arguments: \n' + yaml.dump(vars(args), indent=2))

  # Setting default values for message, project and time_to_live
  if args.message is None:
    args.message = ''
  if args.project is None:
    args.project = 'default'
  if args.time_to_live is None:
    args.time_to_live = 14
  else:
    args.time_to_live = int(args.time_to_live)

  # Updating pipeline config if update_config flag is set and pipeline_config_path is provided
  if args.update_config and args.pipeline_config_path is not None:
    upc = UpdateConfig(args.pipeline_config_path)
    args.pipeline_config_path = upc.apply()
    log.info(args.pipeline_config_path)

  # Setting deployment_type_label to prod if env argument is not provided
  deployment_type_label = 'stage'
  if args.env is not None:
    deployment_type_label = args.env

  # Updating server_config if offline flag is set
  if args.offline:
    os.environ['MONGO_URI'] = 'mongodb://localhost:27017'
    ip, port = Database.getPlatformConfig(deployment_type_label)
    server_config['MOLECULE_UI_URL'] = 'http://' + str(ip) + ':2020/'

  # Creating a CLI object
  cli = CLI(server_config['MOLECULE_UI_URL'])

  # Updating autoscale policy if update_autoscale_policy flag is set
  if args.update_autoscale_policy:
    cli.updateAutoscalePolicy()

  # Submitting or killing pipeline if submit_pipeline or kill_pipeline flag is set
  if args.submit_pipeline or args.kill_pipeline:
    # Setting TTL for files and resolving git
    if args.time_to_live in [14, 28, 60, 120]:
      Metadata.setTTL(args.time_to_live)
    else:
      raise Exception("Allowed values - 14, 28, 60, 120")
      
    Resolve.resolveGit(pipeline_config)
    # Submitting pipeline if submit_pipeline flag is set and pipeline_config is not None
    if args.submit_pipeline and pipeline_config is not None:
      Resolve.resolveGit(pipeline_config)
      local_working_dir = Resolve.resolveWorkingDirectory(pipeline_config, cli.store)
      Resolve.resolveMeta()
      
      if args.offline:
        blob_loc = Resolve.get_working_dir(args.pipeline_name, mode='local')
      else:
        blob_loc = Resolve.get_working_dir(args.pipeline_name, mode='gcp')
      if args.offline:
        Metadata.setWorkingDir(blob_loc)
      else:
        Metadata.setWorkingDir(os.path.join('gs://' + server_config['MOLECULE_BUCKET_NAME'], blob_loc))
      p_data = cli.generatePipeline(args.pipeline_name, args.pipeline_spec_path, args.pipeline_config_path, queue=args.queue,
                      message=args.message, project=args.project)
      # Checking worker policy and syncing code
      log.info('checking worker policy')
      asp = AutoScalingPolicy()
      tg_dict = p_data['task_dict']['tg_dict']
      worker_policy_pass = True
      for t_hash, t_dict in tg_dict.items():
        if not asp.checkWorkerReqPolicy(t_dict['worker_req']):
          worker_policy_pass = False
          print(t_dict['name'] + '_class', t_dict['worker_req'])
          print('Worker Requirements outside limit. Please Fix')
      if not worker_policy_pass:
        print()
        print("LIMITS ARE " + "max_cpus:" + str(asp.max_cpus) + "/max_memory:" + str(asp.max_memory) + \
            "/max_disk_size:" + str(asp.max_disk_size) + "/max_gpus:" + str(asp.max_gpus))
        print()
        log.info("Not submitting pipeline! Exiting.")
        sys.exit()

      log.info('syncing code')
      if args.offline:
        Resolve.syncCode(blob_loc, local_working_dir, mode='local')
      else:
        Resolve.syncCode(blob_loc, local_working_dir, mode='gcp')
      log.info('submitting pipeline!')
      cli.submitPipeline(p_data)
    # Killing pipeline if kill_pipeline flag is set
    if args.kill_pipeline:
      cli.killPipeline(args.pipeline_name,
               args.pipeline_spec_path, args.pipeline_config_path)

  # Deleting dataset if delete_dataset flag is set
  if args.delete_dataset:
    db = Database(deployment_type_label)
    del_resp = db.deleteDatasetHash(os.path.join(Resolve.get_remote_path(), str(args.delete_dataset)))
    if del_resp:
      print("Successfully deleted {} dataset".format(args.delete_dataset))

if __name__ == '__main__':
  main()
