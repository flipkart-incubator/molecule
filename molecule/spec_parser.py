import yaml
import logging
# import mapper_base
from os import listdir
from os.path import join

class SpecParser:
  """
  A static-class to manage transform specs and make classes
  """
  TransformRegistry = {}
  PipelineRegistry = {}

  @staticmethod
  def createTransformType(t_name, t_spec, t_module, t_base, t_expected_fields):
    """
    Creates a class and attaches it to given module, given the following params

    :param t_name: Name of the transform
    :type t_name: str
    :param t_spec: spec for transform
    :type t_spec: dict
    :param t_module: module to attach transform to
    :type t_module: python module
    :param t_base: base class to inherit
    :type t_base: class
    :param t_expected_fields: expected fields in the spec
    :type t_expected_fields: list
    :return: None
    :rtype: None
    """
    # warn if spec not found
    if t_spec is None:
      logging.warning('Spec not available for transform: %s', t_name)
      return

    # create dict to assign class variables
    a_dict = {
      'name': None,
      'spec': t_spec
    }
    for k in t_spec.keys():
      a_dict[k] = t_spec[k]

    # populate expected fields if not in spec
    if k in t_expected_fields:
      if k not in a_dict.keys():
        a_dict[k] = {}

    # set classname to nameSpec
    t_specname = t_name + '_spec'

    if str(t_module.__name__).split('.')[-1] == 'transforms':
      store_dict = SpecParser.TransformRegistry
    elif str(t_module.__name__).split('.')[-1] == 'pipelines':
      store_dict = SpecParser.TransformRegistry

    # if transform already present stop
    if t_name in store_dict.keys():
      raise Exception("Can not have multiple transforms/pipelines with same name: %s", t_name)

    # attach created class to module and add it to registry
    ts_type = type(t_specname, (t_base,), a_dict)
    setattr(t_module, t_specname, ts_type)
    store_dict[t_name] = ts_type

  @staticmethod
  def createTransformTypes(t_path, t_module, t_base, t_expected_fields):
    """
    Reads YAML configs from a location and runs createTransform for each

    :param t_path: yaml location
    :type t_path: str
    :param t_module: module to attach transform to
    :type t_module: python module
    :param t_base: base class to inherit
    :type t_base: class
    :param t_expected_fields: expected fields in the spec
    :type t_expected_fields: list
    :return: None
    :rtype: None
    """
    # Loop over files in the path dir
    for f_name in listdir(t_path):
      if f_name.endswith('.yaml'):
        f = open(join(t_path, f_name), 'r')
        t = yaml.load(f, Loader=yaml.Loader)
        # loop over transforms in a YAML
        for k, v in t.items():
          SpecParser.createTransformType(k, v, t_module, t_base, t_expected_fields)

  # change variable name for spec
  @staticmethod
  def getTransformSpec(t_instance_spec):
    """
    Return instance of created Transform Class

    :param t_instance_spec: name of transform
    :type t_instance_spec: str
    :return: Instance transformSpec class
    :rtype: object
    """
    # get class from spec
    t_class = SpecParser.TransformRegistry[t_instance_spec['type']]
    # create and return instance
    t_instance = t_class(t_instance_spec)
    return t_instance

def setMCClass(_class):
  SpecParser.TransformRegistry['map_combine'] = _class

def setMapper(_class):
  SpecParser.TransformRegistry['mapper'] = _class
