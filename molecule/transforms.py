import sys
import os


from .spec_parser import SpecParser
from . import task
from .resolve import Resolve

current_module = sys.modules[__name__]


class SpecNode:
  """
  Base class for all specification nodes. Provides common functionality for
  loading and parsing specification files.
  """
  def __init__(self, t_instance_spec=None):
    """
    Initializes a new instance of the TransformSpecBase class.

    Args:
      t_instance_spec (dict): The instance specification dictionary.
    """
    self._instance_spec = t_instance_spec
    if hasattr(self, 'params_map') is False:
      self.params_map = None
    # self.params_map = None
    self._initSpec()
    self.planTransform()

  def _initSpec(self):
    """
    Initializes the specification for the current instance of the TransformSpecBase class.
    If an instance specification is provided, it sets the `params_map` attribute to the value of the 'params_map' key
    in the instance specification dictionary.
    """
    if self._instance_spec is not None:
      self.params_map = self._instance_spec.get('params_map', None)

  def planTransform(self):
    """
    This method is responsible for planning the transformation process.
    It should be implemented by the derived class.
    """
    raise Exception("planTransform unimplemented in ", self.__class__.__name__)

  @staticmethod
  def mapParam(t_config, t_config_map, k):
    """
    Maps a parameter from the configuration to a parameter in the specification.

    Args:
      t_config (dict): The configuration dictionary.
      t_config_map (dict): The configuration map dictionary.
      k (str): The key of the parameter to map.

    Returns:
      The value of the mapped parameter.

    Raises:
      Exception: If the parameter map is insufficient or invalid, or if the configuration is insufficient.
    """
    k_map = t_config_map.get(k, None)
    if k_map is None:
      raise Exception("Insufficient Parameter Map", k, k_map)

    if k_map.split('/')[0] not in ['config', 'params']:
      raise Exception("Invalid Params Map", k_map)

    v_map = t_config.get(k_map.split('/')[1], None)
    if v_map is None:
      raise Exception("Insufficient Config", k_map, v_map)

    return v_map

  @staticmethod
  def mapParams(t_spec, t_config, t_config_map):
    """
    Maps parameters from the configuration to parameters in the specification.

    Args:
      t_spec (dict): The specification dictionary.
      t_config (dict): The configuration dictionary.
      t_config_map (dict): The configuration map dictionary.

    Returns:
      A dictionary containing the mapped parameters.

    """
    if t_config_map is None:
      return t_config

    t_params = {}

    for k, v in t_spec['params'].items():
      t_params[k] = TransformSpecBase.mapParam(t_config, t_config_map, k)

    return t_params

  def getTaskGraph(self, in_config, in_input_hashes):
    """
    Returns a task graph and output hashes for the current transform specification.
    """
    raise Exception("getTaskGraph unimplemented in ", self.__class__.__name__)


class TransformSpecBase(SpecNode):
  """
  Base class for all transform specifications. Inherits from SpecNode and provides common functionality for
  planning transforms and creating task graphs.

  Attributes:
    Metadata (dict): A dictionary containing metadata for the transform specification.
  """
  Metadata = None

  def __init__(self, t_instance_spec=None):
    """
    Initializes a new instance of the TransformSpecBase class.

    Args:
      t_instance_spec (dict): A dictionary containing the instance specification.
    """
    super().__init__(t_instance_spec)
    self.t_type = self._instance_spec['type']

  def set_type(self, val):
    """
    Sets the type of the transform.

    Args:
      val (str): The type of the transform.
    """
    self.t_type = val

  def planTransform(self):
    """
    Plans the transform.
    """
    pass

  def getTaskGraph(self, in_config, in_input_hashes):
    """
    Returns a task graph and output hashes for the current transform specification.

    Args:
      in_config (dict): The configuration dictionary.
      in_input_hashes (dict): The input hashes dictionary.

    Returns:
      A tuple containing the task graph and output hashes.
    """
    ti = task.Task(self.name, self.t_type, self.spec, in_config, self.params_map, in_input_hashes)
    # t.compute hashes
    ti.computeHashes()

    tg = task.TaskFactory.newTask(ti, ti.hashes.outputs)

    return tg, ti.hashes.outputs


def loadTransforms(debug=None):
  """
  Loads transform types from the transforms directory and resolves metadata.

  Args:
    debug (bool): If True, enables debug mode.

  Returns:
    None
  """
  SpecParser.createTransformTypes(os.path.join(Resolve.specs_path, 'transforms'), current_module, TransformSpecBase,
                                  ['params', 'inputs', 'outputs'])
  Resolve.resolveMeta(debug=debug)
