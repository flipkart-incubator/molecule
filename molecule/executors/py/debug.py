import os
import yaml
import subprocess
from datetime import datetime
from urllib.parse import urlencode

from molecule.cli import CLI, SpecParser
from molecule.graph import generateGraph
import sys
sys.path.append(os.path.dirname(__file__))
import logger

from molecule.database import Database

_IMPORT_FLAG = False

_store_loc = None
_remote_ip = None
_remote_store_loc = None
_defs_path = None
_store = None

executor = None
storage = None


def dedupe(seq):
  seen = set()
  seen_add = seen.add
  return [x for x in seq if not (x in seen or seen_add(x))]
class Dataset:
  def __init__(self, **kwargs):
    self.__dict__.update(kwargs)

def reimport():
  executor.importAll()

def setOptions(store_loc, remote_ip, remote_store_loc, defs_path, init_store=True, mode='gcs', db_env='stage'):
  global _store_loc, _remote_ip, _remote_store_loc, _defs_path, _IMPORT_FLAG
  _store_loc = store_loc
  _remote_ip = remote_ip

  if remote_store_loc is None:
    remote_store_loc = '/tmp'
  _remote_store_loc = remote_store_loc
  _defs_path = defs_path
  if init_store:
    global executor
    global storage
    import executor
    if _IMPORT_FLAG is False:
      executor.importAll()
      _IMPORT_FLAG = True
    import storage
    global _store
    db = Database(db_env=db_env)
    _store = storage.DebugStorage(store_loc, remote_ip, remote_store_loc, defs_path, mode, db)

def generatePlan(pipeline_name, pipeline_spec_path, pipeline_config_path, debug=None, expand=False, static_names=False):
  SpecParser.TransformRegistry = dict()
  task_dict, _, pipeline = CLI.getTaskDict(pipeline_name, pipeline_spec_path, pipeline_config_path, debug=debug)
  pipeline.graph = generateGraph(task_dict)
  pl_tasks = SpecParser.TransformRegistry[pipeline_name].tasks.keys()
  task_map = dict()
  for task_name in pl_tasks:
    task_map[task_name] = None

  mc_task_hash_list = list()
  mc_task_name_map = dict()
  mc_task_counter = dict()

  for t_hash in task_dict.tg_list:
    task = task_dict.tg_dict[t_hash]
    if task.name in task_map.keys() and task_map[task.name] is None:
      task_map[task.name] = t_hash
    elif expand:
      if static_names:
        mc_task_hash_list.append(t_hash)
        if mc_task_name_map.get(task.name, None) is None:
          mc_task_name_map[task.name] = [t_hash]
          mc_task_counter[task.name] = 0
        else:
          mc_task_name_map[task.name].append(t_hash)
      else:
        task_map[t_hash] = t_hash
  
  if len(mc_task_hash_list) > 0:
    for mc_task_name, mc_task_list in mc_task_name_map.items():
      mc_task_list = dedupe(mc_task_list)
      if len(mc_task_list) > 1:
        for mc_task in mc_task_list:
          counter = mc_task_counter[mc_task_name]
          task_map['_'.join([mc_task_name, str(counter+1)])] = mc_task
          mc_task_counter[mc_task_name] = counter + 1
      else:
        task_map[mc_task_name] = mc_task_list[0]
  
  plan = Plan(task_map, task_dict, pipeline)
  return plan

class Plan:
  def __init__(self, task_map, task_dict, pipeline=None):
    self.task_hash_map = task_map
    self.hash_task_map = {v: k for k, v in task_map.items()}
    self.task_dict = task_dict
    self.pipeline = pipeline
    for task_name, t_hash in self.task_hash_map.items():
      if t_hash is None:
        continue
      self.__dict__.update({
        task_name: TaskDebugger(task_dict.tg_dict[t_hash])
      })

  def getExecOrder(self):
    exec_order = list()
    for t_hash in self.task_dict.tg_list:
      if t_hash in self.hash_task_map.keys():
        exec_order.append(self.hash_task_map[t_hash])
      else:
        exec_order.append(t_hash)
    return exec_order

  def execTill(self, task_name):
    exec_order = self.getExecOrder()
    for current_task in exec_order:
      if current_task == task_name:
        break
      if current_task in self.__dict__.keys():
        tdi = getattr(self, current_task)
      else:
        tdi = TaskDebugger(self.task_dict.tg_dict[current_task])
      if tdi.task.class_language.lower() != 'py':
        print('Skipping ', tdi.task.name, ' run because class language is ', tdi.task.class_language)
      else:
        tdi.loadInputs()
        tdi.run()
        tdi.saveOutputs()

class TaskDebugger:
  def __init__(self, task):
    self.task = task
    self.t_hash_dict = task.hashes.serializeDict()
    self.inputs = None
    self.params = None
    self.outputs = None
    self.ti = None
    self.log = logger.getLogger(self.t_hash_dict['transform_hash'], stderr=True, enable_cloud_logging=False)
    if _IMPORT_FLAG:
      inputs_path = executor.loadPaths(_store, self.t_hash_dict)
      outputs_path = executor.loadPaths(_store, self.t_hash_dict, 'outputs')
      self.context = Dataset(logger=self.log, paths=(inputs_path, outputs_path))
    else:
      self.context = Dataset(logger=self.log)

  def loadInputs(self):
    self.inputs, self.params = executor.loadInputs(_store, self.t_hash_dict)

  def apply(self):
    if self.task.class_language.lower() != 'py':
      print('cannot run', self.task.class_language, 'in Python')
      return
    self.ti = executor.getInstance(self.t_hash_dict, self.inputs, self.params, context=self.context)
    if _store.local_store.db != None:
      t_dict = _store.local_store.db.getTaskFromDb(self.t_hash_dict['transform_hash'])
      if not _store.local_store.db.addTransformHashesToDb(t_dict):
        print("Adding output hash to dataset DB failed")
    self.ti.apply()
    self.outputs = self.ti.outputs

  def run(self):
    if self.task.class_language.lower() != 'py':
      print('cannot run', self.task.class_language, 'in Python')
      return
    self.ti = executor.getInstance(self.t_hash_dict, self.inputs, self.params, context=self.context)
    try:
      self.ti.apply()
    except Exception as err:
      print('Did you run loadInputs() method before run?')
      print(self.task.name, " failed to apply, please check and run again")
      print(err)
      return
    self.outputs = self.ti.outputs

  def loadOutputs(self):
    self.outputs = executor.loadOutputs(_store, self.t_hash_dict)

  def saveOutputs(self):
    if self.outputs is None:
      print('cannot save empty outputs')
      return
    executor.saveOutputs(_store, self.t_hash_dict, self)

  def saveOutputsToFile(self):
    if self.outputs is None:
      print('cannot save empty outputs')
      return
    executor.saveOutputsToFile(_store, self.t_hash_dict, self)

  def deleteInputs(self):
    for _, inp_hash in self.t_hash_dict['inputs'].items():
      _store.delete(inp_hash)

  def deleteOutputs(self):
    for _, out_hash in self.t_hash_dict['outputs'].items():
      _store.delete(out_hash)

  def getLog(self):
    base_url = 'https://console.cloud.google.com/logs/query;query='
    query_params = {
      'logName': 'projects/' + os.environ['GOOGLE_CLOUD_PROJECT'] + '/logs/' + str(self.t_hash_dict['transform_hash']),
      'labels.deployment_type': _store.local_store.db.db_env
    }
    user_related_dir_string = _remote_store_loc.split('users/')[1]
    user_name = user_related_dir_string.split('/')[0]
    query_params['labels.user'] = user_name
    url = base_url + urlencode(query_params) + '?project=' + os.environ['GOOGLE_CLOUD_PROJECT']
    url = url.replace('&', '%0A')
    print("Check logs here", url)
    
  # Deprecated
  def getLogListing(self):
    self.getLog()
    # global _store_loc
    # log_dir = os.path.join(_store_loc, 'logs', self.task.hashes.transform_hash)
    # timestamps = os.listdir(log_dir)
    # timestamps.sort(reverse=True)
    # logs = dict()
    # for timestamp in timestamps:
    #   logs[timestamp] = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    # print(yaml.dump(logs, indent=2))

