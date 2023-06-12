import hashlib
import yaml

from . import graph
from .metadata import Metadata

yaml.Dumper.ignore_aliases = lambda *args : True

class TransformHashes:
  """
  A class that represents the hashes of the inputs, outputs, and parameters of a transform.
  """
  def __init__(self):
    """
    Initializes the hashes of the inputs, outputs, and parameters of a transform.
    """
    self.inputs = {}
    self.outputs = {}
    self.params = {}
    self.transform = {}
    self.inputs_type = {}
    self.outputs_type = {}
    self.transform_hash = None

  def serializeDict(self):
    """
    Returns a dictionary containing the hashes of the inputs, outputs, and parameters of a transform, along with the transform itself.
    """
    t_hash_dict = {
      'inputs': self.inputs,
      'inputs_type': self.inputs_type,
      'outputs': self.outputs,
      'outputs_type': self.outputs_type,
      'params': self.params,
      'transform': self.transform,
      'transform_hash': self.transform_hash
    }

    return t_hash_dict
  
  def populate(self, t_hash_dict):
    """
    Populates the hashes of the inputs, outputs, and parameters of a transform, along with the transform itself, from a dictionary.

    Args:
      t_hash_dict: A dictionary containing the hashes of the inputs, outputs, and parameters of a transform, along with the transform itself.
    """
    self.inputs = t_hash_dict['inputs']
    self.inputs_type = t_hash_dict['inputs_type']
    self.outputs = t_hash_dict['outputs']
    self.outputs_type = t_hash_dict['outputs_type']
    self.params = t_hash_dict['params']
    self.transform = t_hash_dict['transform']
    self.transform_hash = t_hash_dict['transform_hash']


class TaskGroupBase:
  """
  A base class for task groups.
  """
  def __init__(self):
    """
    Initializes the TaskGroupBase object.
    """
    self.serialize = False
    pass

  def addTaskGroup(self, t_name, task, out_hashes):
    """
    Adds a task group to the TaskGroupBase object.

    Args:
      t_name: A string representing the name of the task.
      task: A Task object representing the task.
      out_hashes: A dictionary representing the output hashes of the task.
    """
    raise Exception("addTaskGroup called incorrectly in: ", self.__class__.__name__)

  def addDep(self, t_name, child_t_name):
    """
    Adds a dependency to the TaskGroupBase object.

    Args:
      t_name: A string representing the name of the task.
      child_t_name: A string representing the name of the child task.
    """
    raise Exception("addDep called incorrectly in: ", self.__class__.__name__)

  def serializeDict(self):
    """
    Serializes the TaskGroupBase object to a dictionary.
    """
    raise Exception("serialize called incorrectly in: ", self.__class__.__name__)

  def markSerialize(self):
    """
    Marks the TaskGroupBase object as serialized.
    """
    self.serialize = True

  def isSerialized(self):
    """
    Returns True if the TaskGroupBase object is serialized, False otherwise.
    """
    return self.serialize


class Task(TaskGroupBase):
  """
  A class that represents a task in a molecule workflow.

  Inherits from TaskGroupBase.
  """
  def __init__(self, t_name, t_type, t_spec, t_config, t_config_map, t_input_hashes):
    """
    Initializes a Task object.

    Args:
      t_name: A string representing the name of the task.
      t_type: A string representing the type of the task.
      t_spec: A dictionary representing the specification of the task.
      t_config: A dictionary representing the configuration of the task.
      t_config_map: A dictionary representing the configuration map of the task.
      t_input_hashes: A dictionary representing the input hashes of the task.
    """
    self.name = t_name
    self.t_type = t_type
    self.spec = t_spec
    self.hashes = TransformHashes()
    self.params = {}
    self.inputs = {}
    self.outputs = {}

    self._cpu = 4
    self._memory = '16G'
    self._disk_size = '256G'
    self._gpu = 0
    self._exact_machine_type = ''

    if self.spec.get('params', None) is not None:
      for k, v in self.spec['params'].items():
        k_map = t_config_map.get(k, None)
        if k_map is None:
          raise Exception("Insufficient Parameter Map", k)

        v_map = t_config.get(k_map.split('/')[1], None)
        if v_map is None:
          raise Exception("Insufficient Config", k_map)

        self.params[k] = v_map
        self.hashes.params[k] = v_map

    if self.spec.get('inputs', None) is not None:
      for k, v in self.spec['inputs'].items():
        i_hash = t_input_hashes.get(k, None)
        if i_hash is None:
          raise Exception("Insufficient Inputs")

        self.inputs[k] = {
          'hash_key': i_hash,
          'type': v
        }
        self.hashes.inputs_type[k] = v
        self.hashes.inputs[k] = i_hash

    implementation_spec_list = []
    if self.name == 'combiner':
      implementation_spec_list = self.params['combiner_class'].split('/')
      self.name = self.t_type
      # Set the value of combiner_class after removing the worker requirements. So that hash dosn't change
      trimmed_worker_req_value = '/'.join(implementation_spec_list[:2])
      self.params['combiner_class'] = trimmed_worker_req_value
      self.hashes.params['combiner_class'] = trimmed_worker_req_value
    else:
      implementation_spec_list = t_config[self.name+'_class'].split('/')

    lang, c_name = implementation_spec_list[0], implementation_spec_list[1]
    if len(implementation_spec_list) > 2:
      cpu = implementation_spec_list[2]
      if cpu != '':
        self._cpu = cpu
    if len(implementation_spec_list) > 3:
      memory = implementation_spec_list[3]
      if memory != '':
        self._memory = memory
    if len(implementation_spec_list) > 4:
      disk_size = implementation_spec_list[4]
      if disk_size != '':
        self._disk_size = disk_size
    if len(implementation_spec_list) > 5:
      gpu = implementation_spec_list[5]
      if gpu != '':
        self._gpu = gpu
    if len(implementation_spec_list) > 6:
      exact_machine_type = implementation_spec_list[6]
      if exact_machine_type != '':
        self._exact_machine_type = exact_machine_type
        self._cpu = '1'
        self._memory = '2G'

    self._class_name = c_name
    self._language = str(lang).lower()
    self._metadata = Metadata.MetadataRegistry[self._language][self._class_name]
    self._affinity = self._metadata['affinity']
    self._version = self._metadata['version']
    self._git_commit_id = Metadata.GitCommitId
    self._working_dir = Metadata.WorkingDir
    self._debug = Metadata.Debug
    self._data_dir = Metadata.DataDir
    self._ttl = Metadata.TTL
    if self._affinity != 'gpu':
      self._gpu = 0
    if self._language in ['hive', 'bq']:
      self._ttl = 120
    if Metadata.TTL > self._ttl:
      self._ttl = Metadata.TTL
    # Handle TTL None in gcs_serde in prod
    if self._git_commit_id is not None:
      self._ttl = None

  @property
  def class_name(self):
    # FIXME: parse from actual file docstring
    return self._class_name

  @property
  def class_version(self):
    # FiXME: parse from actual file docstring
    return self._version

  @property
  def class_language(self):
    return self._language

  @property
  def class_affinity(self):
    return self._affinity

  @property
  def git_commit_id(self):
    return self._git_commit_id
  
  @property
  def debug(self):
    return self._debug

  @property
  def working_dir(self):
    return self._working_dir

  @property
  def data_dir(self):
    return self._data_dir

  @property
  def ttl(self):
    return self._ttl

  @property
  def cpu(self):
    return self._cpu

  @property
  def memory(self):
    return self._memory

  @property
  def disk_size(self):
    return self._disk_size

  @property
  def gpu(self):
    return self._gpu
  
  @property
  def exact_machine_type(self):
    return self._exact_machine_type

  # not required as of now
  @property
  def in_deps(self):
    pass

  # not required as of now
  @property
  def out_deps(self):
    pass

  @property
  def docker_container(self):
    return None

  def populateHashDict(self):
    """
    Populates the hash dictionary with the necessary information for computing the hash of the transform.

    Raises:
        Exception: If class_name or class_version is unknown.
    """
    if self.class_name is None or self.class_version is None:
      raise Exception("Unknown class_name or class_version")

    # fix properly
    self.hashes.transform['transform_class'] = self.class_name
    self.hashes.transform['transform_type'] = self.t_type
    self.hashes.transform['transform_class_version'] = self.class_version
    self.hashes.transform['transform_class_language'] = self.class_language
    self.hashes.transform['transform_class_affinity'] = self.class_affinity
    self.hashes.transform['git_commit_id'] = None
    # self.hashes.transform['working_dir'] = self.working_dir

  def computeHashes(self):
    """
    Computes the hashes for the task by populating the hash dictionary with the necessary information for computing the hash of the transform, computing the hash, and populating the output hashes.

    Raises:
      Exception: If class_name or class_version is unknown.
    """
    self.populateHashDict()

    # compute hash. fix properly
    t_hash = hashlib.md5(yaml.dump(
      {'params': self.hashes.params, 'inputs': self.hashes.inputs,
       'transform': self.hashes.transform}).encode('utf-8')).hexdigest()
    self.hashes.transform_hash = t_hash

    self.populateOutputHashes()

  def populateOutputHashes(self):
    """
    Populates the output hashes for the task by computing the hash for each output and adding it to the hash dictionary.

    Raises:
      None
    """
    if self.spec.get('outputs', None) is not None:
      for k, v in self.spec['outputs'].items():
        o_hash = hashlib.md5((self.hashes.transform_hash + str(k)).encode('utf-8')).hexdigest()
        self.hashes.outputs[k] = o_hash
        self.hashes.outputs_type[k] = v
        self.outputs[k] = {
          'hash_key': o_hash,
          'type': v
        }

  def run(self):
    raise Exception("Run method not implemented")

  def schedule(self):
    raise Exception("Schedule not implemented")

  def serializeDict(self):
    """
    Serializes the task object into a dictionary.

    Returns:
      dict: A dictionary containing the serialized task object.
    """
    task_dict = {
      'name': self.name,
      'class_name': self.class_name,
      'class_version': self.class_version,
      'class_affinity': self.class_affinity,
      'class_language': self.class_language,
      'git_commit_id': self.git_commit_id,
      'working_dir': self.working_dir,
      'ttl': self.ttl,
      'docker_container': self.docker_container,
      'hashes': self.hashes.serializeDict(),
      'inputs': self.inputs,
      'outputs': self.outputs,
      'params': self.params,
      'worker_req': {
        'cpu': self.cpu,
        'memory': self.memory,
        'disk_size': self.disk_size,
        'gpu': self.gpu,
        'exact_machine_type': self.exact_machine_type,
        'affinity': self.class_affinity
      },
      'data_dir': self.data_dir
    }

    return task_dict

class SeqGroup(TaskGroupBase):
  """
  A class representing a sequential group of tasks.

  Attributes:
    dep_count (dict): A dictionary of task hashes and their corresponding dependency count.
    ds_deps (dict): A dictionary of task hashes and their corresponding downstream dependencies.
    tg_dict (dict): A dictionary of task hashes and their corresponding tasks in the sequential group.
    tg_list (list): A list of task hashes in the sequential group.
  """
  def __init__(self):
    self.git_url = Metadata.GitUrl
    self.git_commit_id = Metadata.GitCommitId
    self.dep_count = {}
    self.ds_deps = {}
    self.tg_dict = {}
    self.tg_list = []

  def addTaskGroup(self, tg_name, tg, out_hashes):
    """
    Adds a task group to the current task.

    Args:
      tg_name (str): The name of the task group.
      tg (SeqGroup): The task group to add.
      out_hashes (dict): A dictionary of output hashes for the task group.

    Returns:
      None
    """
    if not isinstance(tg, SeqGroup):
      self._addTask(tg)
    else:
      for t_hash in tg.tg_list:
        self._addTask(tg.tg_dict[t_hash])

  def addDep(self, t_name, child_t_name):
    pass

  def _addTask(self, ti):
    """
    Adds a task to the current task group.

    Args:
      ti (Task): The task to add.
    """
    if not isinstance(ti, Task):
      return

    self.tg_dict[ti.hashes.transform_hash] = ti
    self.tg_list.append(ti.hashes.transform_hash)

    if ti.t_type[-9:] == '_combiner':
      dep_count = sum(len(set(v.values())) for v in ti.hashes.inputs.values())
      self.dep_count[ti.hashes.transform_hash] = dep_count
      for k, in_hash_dict in ti.hashes.inputs.items():
        for in_hash in in_hash_dict.values():
          if in_hash not in self.ds_deps.keys():
            self.ds_deps[in_hash] = []
          self.ds_deps[in_hash].append(ti.hashes.transform_hash)
          self.ds_deps[in_hash].sort()
    else:
      self.dep_count[ti.hashes.transform_hash] = len(ti.hashes.inputs.keys())
      for k, in_hash in ti.hashes.inputs.items():
        if in_hash not in self.ds_deps.keys():
          self.ds_deps[in_hash] = []
        self.ds_deps[in_hash].append(ti.hashes.transform_hash)
        self.ds_deps[in_hash].sort()

  def serializeDict(self):
    """
    Serializes the current task group into a dictionary.

    Returns:
      dict: A dictionary containing the serialized task group.
    """
    serialized_dict = {
      'git_url': self.git_url,
      'git_commit_id': self.git_url,
      'dep_count': self.dep_count,
      'ds_deps': self.ds_deps,
      'tg_dict': {},
      'tg_list': self.tg_list
    }
    for k, v in self.tg_dict.items():
      serialized_dict['tg_dict'][k] = v.serializeDict()

    return serialized_dict

class GraphGroup(TaskGroupBase):
  """
  A class representing a group of tasks that are organized in a graph structure.

  Attributes:
    tg_graph (Graph): A graph object representing the task group.
  """
  def __init__(self):
    self.tg_graph = graph.Graph()

  def addTaskGroup(self, tg_name, tg, out_hashes):
    """
    Adds a task group to the graph group.

    Args:
      tg_name (str): The name of the task group.
      tg (TaskGroupBase): The task group to add.
      out_hashes (list): A list of output hashes for the task group.
    """
    self.tg_graph.addNode(tg_name, tg)
    if tg.isSerialized():
      tg.markSerialize()

  def addDep(self, t_name, child_t_name):
    """
    Adds a dependency between two tasks in the graph group.

    Args:
      t_name (str): The name of the task that the dependency is being added to.
      child_t_name (str): The name of the task that is being added as a dependency.
    """
    self.tg_graph.addDep(child_t_name, t_name, None)

class TaskFactory:
  """
  A factory class for creating tasks and task groups.

  Attributes:
    TaskGroupType (TaskGroupBase): The type of task group to create.
  """
  TaskGroupType = SeqGroup

  @staticmethod
  def initTaskGroupType(jtype):
    TaskFactory.TaskGroupType = jtype

  @staticmethod
  def newTask(task_obj, out_hashes):
    return task_obj

  @staticmethod
  def newGroup():
    return TaskFactory.TaskGroupType()
