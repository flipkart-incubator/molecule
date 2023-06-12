import sys
import os
import yaml

from . import graph, transforms, spec_parser, task
from .resolve import Resolve

# Try to import mappers module from operators package
# and append the working directory to sys.path
try:
  sys.path.append(os.path.expanduser(Resolve.wd))
  import operators.mappers as mappers
except:
  pass

# Import pl_logger module and create a logger object
from . import pl_logger as logger
log = logger.getLogger('pipelines', enable_cloud_logging=False)

# Get the current module object
current_module = sys.modules[__name__]


class PipelineSpecBase(transforms.SpecNode):
  """
  Base class for pipeline specifications. Inherits from SpecNode.
  """
  INPUTS_KEY = 'inputs'
  OUTPUTS_KEY = 'outputs'

  def __init__(self, t_instance_spec=None):
    """
    Initializes a new instance of the TransformInstance class.

    Args:
      t_instance_spec (dict): A dictionary containing the specification for the transform instance.
    """
    super().__init__(t_instance_spec)

  def serializeDict(self):
    """
    Serializes the pipeline object into a dictionary.

    Returns:
      dict: A dictionary containing the serialized pipeline object.
    """
    pipeline_dict = {
      'deps': self.deps,
      'inputs': self.inputs,
      'outputs': self.outputs,
      'params': self.params,
      'params_map': self.params_map,
      'spec': self.spec,
      'tasks': self.tasks,
      'type': self.type
    }
    return pipeline_dict

  def getTaskGraph(self, in_config, in_input_hashes):
    """
    Generates a task graph for the transform instance.

    Args:
      in_config (dict): A dictionary containing the input configuration for the transform instance.
      in_input_hashes (dict): A dictionary containing the input hashes for the transform instance.

    Returns:
      tuple: A tuple containing the task graph and the output hashes for the transform instance.
    """
    task_ordered_list = task.TaskFactory.newGroup()
    input_hash_cache = {}
    t_config = transforms.SpecNode.mapParams(self.spec, in_config, self.params_map)
    exec_order = self.getExecOrder()

    for t_name in exec_order:
      if t_name in [PipelineSpecBase.OUTPUTS_KEY, PipelineSpecBase.INPUTS_KEY]:
        for lhs_t_name, deps_list in self.getDepsR(t_name).items():
          task_ordered_list.addDep(lhs_t_name, t_name)
          for df_deps in deps_list:
            lhs_df, rhs_df = df_deps
            if lhs_t_name not in input_hash_cache.keys():
              input_hash_cache[lhs_t_name] = {}
            self.syncInputHashes(input_hash_cache[lhs_t_name], in_input_hashes, lhs_df, rhs_df)
        continue
      if t_name not in input_hash_cache:
        input_hash_cache[t_name] = {}
      log.info('planning ' + t_name)
      # get transform instance/task for t
      # ti = t.getTransformInstance(self.pipeline_config)

      t = self.getTransform(t_name)
      t.name = t_name

      # t.sync input hashes
      in_input_hashes = input_hash_cache[t_name]

      # t.createTaskGraph
      ti, ti_output_hashes = t.getTaskGraph(t_config, in_input_hashes)
      # create newJob if needed
      # add Task to current job if needed

      # t.sync output hashes
      for lhs_t_name, deps_list in self.getDepsR(t_name).items():
        task_ordered_list.addDep(lhs_t_name, t_name)
        for df_deps in deps_list:
          lhs_df, rhs_df = df_deps
          if lhs_t_name not in input_hash_cache.keys():
            input_hash_cache[lhs_t_name] = {}

          # input_hash_cache[child_t_name][c_df] = ti_output_hashes[p_df]
          self.syncInputHashes(input_hash_cache[lhs_t_name], ti_output_hashes, lhs_df, rhs_df)
        if lhs_t_name == PipelineSpecBase.OUTPUTS_KEY:
          ti.markSerialize()
          # FIXME:
        # If parent and child has different languages markSerialize       

      # Add ti to task_graph
      # task_ordered_list.extend(ti)
      task_ordered_list.addTaskGroup(t_name, ti, ti_output_hashes)

    return task_ordered_list, input_hash_cache[PipelineSpecBase.OUTPUTS_KEY]

  def syncInputHashes(self, lhs_dict, rhs_dict, lhs_df, rhs_df):
    raise Exception("syncInputHashes undefined in ", self.__class__.__name__)

  def getTransform(self, in_t_name):
    raise Exception("getTransform undefined in ", self.__class__.__name__)

  def getDeps(self, in_t_name):
    raise Exception("getDeps undefined in ", self.__class__.__name__)

  def getDepsR(self, in_t_name):
    raise Exception("getDepsR undefined in ", self.__class__.__name__)

  def getExecOrder(self):
    raise Exception("getExecOrder undefined in ", self.__class__.__name__)


class PipelineDAG(PipelineSpecBase):
  """
  A class representing a directed acyclic graph (DAG) pipeline specification.
  Inherits from PipelineSpecBase.
  """

  def __init__(self, t_instance_spec=None):
    """
    Initializes a PipelineSpecBase object.

    Args:
      t_instance_spec (dict): A dictionary containing the specification for the pipeline instance.
    """
    self.graph = graph.Graph()
    super().__init__(t_instance_spec)

  def planTransform(self):
    """
    Populates the graph with nodes and edges based on the pipeline specification.
    """
    self.populateGraph()

  def populateGraph(self):
    """
    Populates the graph with nodes and edges based on the pipeline specification.
    """
    # FIXME: outputs is a reserved keyword for toks[0]
    # TODO: Should INPUT/OUTPUT node be associated with self?
    self.graph.addNode(PipelineSpecBase.INPUTS_KEY, self)
    self.graph.addNode(PipelineSpecBase.OUTPUTS_KEY, self)
    for k, v in self.spec['tasks'].items():
      self.graph.addNode(k, spec_parser.SpecParser.getTransformSpec(v))

    if self.spec.get('deps', None) is not None:
      for k, v in self.spec['deps'].items():
        k_toks = k.strip().split('.')
        v_toks = v.strip().split('.')

        # assumes k_toks[0] and v_toks[0] are transform names
        lhs_name = k_toks[0]
        rhs_name = v_toks[0]
        lhs_df = k_toks[1]
        rhs_df = v_toks[1]
        self.graph.addDep(lhs_name, rhs_name, (lhs_df, rhs_df))

  def syncInputHashes(self, lhs_dict, rhs_dict, lhs_df, rhs_df):
    """
    Synchronizes the input hashes between two dataframes.

    Args:
      lhs_dict (dict): A dictionary containing the input hashes for the left-hand side dataframe.
      rhs_dict (dict): A dictionary containing the input hashes for the right-hand side dataframe.
      lhs_df (str): The name of the left-hand side dataframe.
      rhs_df (str): The name of the right-hand side dataframe.
    """
    lhs_dict[lhs_df] = rhs_dict[rhs_df]

  def getTransform(self, in_t_name):
    """
    Returns the transform object for the specified transform name.

    Args:
      in_t_name (str): The name of the transform.

    Returns:
      object: The transform object for the specified transform name.
    """
    return self.graph.nodes[in_t_name]

  def getDeps(self, in_t_name):
    """
    Returns the dependencies of the specified transform.

    Args:
      in_t_name (str): The name of the transform.

    Returns:
      list: A list of dependencies for the specified transform.
    """
    return self.graph.r_edges[in_t_name]

  def getDepsR(self, in_t_name):
    """
    Returns the reverse dependencies of the specified transform.

    Args:
      in_t_name (str): The name of the transform.

    Returns:
      list: A list of reverse dependencies for the specified transform.
    """
    return self.graph.f_edges[in_t_name]

  def getExecOrder(self):
    """
    Returns the execution order of the transforms in the pipeline.

    Returns:
      list: A list of transform names in the order they should be executed.
    """
    return graph.TopSort(self.graph)

  def getTaskGraph(self, in_config, in_input_hashes):
    """
    Returns the task graph for the pipeline.

    Args:
      in_config (dict): The configuration for the pipeline.
      in_input_hashes (dict): The input hashes for the pipeline.

    Returns:
      object: The task graph for the pipeline.
    """
    return super().getTaskGraph(in_config, in_input_hashes)


class PipelineMapCombine(PipelineSpecBase):
  """
  A class representing a pipeline specification that includes a mapper and a combiner.
  Inherits from PipelineSpecBase.
  """

  MAPPER_KEY = 'mappers'
  COMBINER_KEY = 'combiner'

  def __init__(self, t_instance_spec=None):
    """
    Initializes a new instance of the PipelineMapCombine class.

    Args:
      t_instance_spec (dict): The specification for the pipeline.
    """
    self.spec = t_instance_spec
    self.mappers = t_instance_spec[PipelineMapCombine.MAPPER_KEY]
    self.combiner = t_instance_spec[PipelineMapCombine.COMBINER_KEY]
    self.map_config = dict()
    self.mi = dict()
    self.ci = None
    self.exec_order = []
    self.graph = graph.Graph()
    super().__init__(t_instance_spec)

  def populateGraph(self):
    """
    Populates the graph with nodes and edges for the pipeline.
    """
    # FIXME: outputs is a reserved keyword for toks[0]
    self.graph.addNode(PipelineSpecBase.INPUTS_KEY, self)
    self.exec_order.append(PipelineSpecBase.INPUTS_KEY)
    self.graph.addNode(PipelineSpecBase.OUTPUTS_KEY, self)
    self.graph.addNode(PipelineMapCombine.COMBINER_KEY, self.ci)
    for m_name, map_obj in self.mi.items():
      self.graph.addNode(m_name, map_obj)
      self.exec_order.append(m_name)
      for k in map_obj.collect_map.keys():
        self.graph.addDep(PipelineMapCombine.COMBINER_KEY, m_name, (k, k))
      if map_obj.pipeline_inputs is not None:
        for k in map_obj.pipeline_inputs.keys():
          self.graph.addDep(m_name, PipelineSpecBase.INPUTS_KEY, (k, k))

    self.exec_order.append(PipelineMapCombine.COMBINER_KEY)
    for k in self.ci.outputs.keys():
      self.graph.addDep(PipelineSpecBase.OUTPUTS_KEY, PipelineMapCombine.COMBINER_KEY, (k, k))

  def planTransform(self):
    """
    Plans the transformation process for the pipeline by parsing the mapper and combiner specifications,
    creating corresponding objects, and populating the graph with nodes and edges.
    """
    # for each mapper
    for m_name, m_spec in self.mappers.items():
      # m_class_name = m_spec['type']
      # m_class = MapperBase.getMapperClass(m_class_name)
      m_obj = spec_parser.SpecParser.getTransformSpec(m_spec)
      self.mi[m_name] = m_obj

    self.ci = spec_parser.SpecParser.getTransformSpec(self.combiner)
    # self.ci.set_type('combiner')

    self.populateGraph()

  def getTransform(self, in_t_name):
    """
    Returns the transform object associated with the given transform name.

    Args:
      in_t_name (str): The name of the transform.

    Returns:
      The transform object associated with the given transform name.
    """
    return self.graph.nodes[in_t_name]

  def getDeps(self, in_t_name):
    """
    Returns the dependencies of the given transform.

    Args:
      in_t_name (str): The name of the transform.

    Returns:
      The dependencies of the given transform.
    """
    return self.graph.r_edges[in_t_name]

  def getDepsR(self, in_t_name):
    """
    Returns the reverse dependencies of the given transform.

    Args:
      in_t_name (str): The name of the transform.

    Returns:
      The reverse dependencies of the given transform.
    """
    return self.graph.f_edges[in_t_name]

  def getExecOrder(self):
    """
    Returns the execution order of the transforms in the pipeline.

    Returns:
      The execution order of the transforms in the pipeline.
    """
    return self.exec_order

  def syncInputHashes(self, lhs_dict, rhs_dict, lhs_df, rhs_df):
    """
    Synchronizes the input hashes of two dataframes.

    Args:
      lhs_dict (dict): The dictionary containing the input hashes of the left-hand side dataframe.
      rhs_dict (dict): The dictionary containing the input hashes of the right-hand side dataframe.
      lhs_df (str): The name of the left-hand side dataframe.
      rhs_df (str): The name of the right-hand side dataframe.
    """
    if type(rhs_dict[rhs_df]) is not dict:
      lhs_dict[lhs_df] = rhs_dict[rhs_df]
    else:
      if lhs_df not in lhs_dict.keys():
        lhs_dict[lhs_df] = {}
      for k, v in rhs_dict[rhs_df].items():
        lhs_dict[lhs_df][k] = v

  def getTaskGraph(self, in_config, in_input_hashes):
    """
    Returns the task graph for the transform.

    Args:
      in_config (dict): The configuration for the transform.
      in_input_hashes (dict): The input hashes for the transform.

    Returns:
      The task graph for the transform.
    """
    return super().getTaskGraph(in_config, in_input_hashes)


class MetaTransform(transforms.SpecNode):
  """
  A class representing a meta transform.
  Inherits from SpecNode.
  """
  CONFIG_KEY = 'configs'
  MAP_FN_KEY = 'mapper_function'
  PIPELINE_KEY = 'pipeline'
  PIPELINE_CONFIG_KEY = 'pipeline_config'

  def __init__(self, t_instance_spec):
    """
    Initializes a new instance of the Mapper class.

    Args:
      t_instance_spec (dict): The instance specification for the Mapper.

    Raises:
      Exception: If the instance specification is None.
    """
    if t_instance_spec is not None:
      self.params_map = t_instance_spec['params_map']
      self.collect_map = t_instance_spec['collect']
      self.pipeline_inputs = t_instance_spec.get('pipeline_inputs', None)
    else:
      raise Exception("Mapper can not have None Instance Spec: ", self.__class__.__name__)

  def getTaskGraph(self, in_config, in_input_hashes):
    """
    Returns the task graph for the transform.

    Args:
      in_config (dict): The configuration for the transform.
      in_input_hashes (dict): The input hashes for the transform.

    Returns:
      The task graph for the transform.
    """
    # p stands for sub pipeline
    p_name = transforms.SpecNode.mapParam(in_config, self.params_map, MetaTransform.PIPELINE_KEY)
    p_spec = {'type': p_name, 'params_map': self.params_map}
    p_obj = spec_parser.SpecParser.getTransformSpec(p_spec)

    # is it p_spec of p_obj.t_spec?
    with open(transforms.SpecNode.mapParam(in_config, self.params_map, MetaTransform.PIPELINE_CONFIG_KEY)) as f:
      p_file_config = yaml.load(f, Loader=yaml.Loader)
    p_config = p_obj.mapParams(p_obj.spec, p_file_config, p_obj.spec.get('params_map', None))
    # p_config = {p_name+'%%%'+str(k): v for k, v in p_config}

    p_input_hashes = in_input_hashes
    task_ordered_list = task.TaskFactory.newGroup()
    p_output_hashes = {
      MetaTransform.CONFIG_KEY: dict()
    }

    map_fn_key = transforms.SpecNode.mapParam(in_config, self.params_map, MetaTransform.MAP_FN_KEY)
    map_params = [x for x in self.params_map.keys() if x not in [MetaTransform.MAP_FN_KEY, MetaTransform.PIPELINE_KEY, MetaTransform.PIPELINE_CONFIG_KEY]]
    map_config = {k: v for k, v in in_config.items() if k in map_params}
    map_fn = getattr(mappers, map_fn_key)
    map_args = type('map_args', (object,), map_config)
    gen_configs = map_fn(map_args, p_config)

    for m_tuple in gen_configs.items():
      m_instance_name, m_config = m_tuple
      p_obj.params_map = None
      ti, hashes = p_obj.getTaskGraph(m_config, p_input_hashes)
      # task_ordered_list[m_name] = ti
      task_ordered_list.addTaskGroup(m_instance_name, ti, hashes)
      # collect outputs
      p_output_hashes[MetaTransform.CONFIG_KEY][m_instance_name] = m_config
      for k, v in self.collect_map.items():
        if k not in p_output_hashes.keys():
          p_output_hashes[k] = {}
        p_output_hashes[k][m_instance_name] = hashes[v.split('.')[1]]

    # FIX: task_ordered_list should be union of lists, not dict
    return task_ordered_list, p_output_hashes


# TODO: project restructuring for pipelines
def loadPipeline(pipeline_spec_path):
  """
  Loads a pipeline specification from the given path and creates transform types for the current module.

  Args:
    pipeline_spec_path (str): The path to the pipeline specification.

  Returns:
    None
  """
  spec_parser.SpecParser.createTransformTypes(pipeline_spec_path, current_module, PipelineDAG,
                                              ['params', 'inputs', 'outputs', 'tasks', 'deps'])

  spec_parser.setMCClass(PipelineMapCombine)
  spec_parser.setMapper(MetaTransform)
