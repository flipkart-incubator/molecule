import os
import glob
import yaml
import argparse

from . import pl_logger as logger

log = logger.getLogger('metadata', enable_cloud_logging=False)

def readYAMLs(path):
  """
  Reads all YAML files in the specified directory and returns a dictionary containing the merged contents.

  Args:
  - path (str): The path to the directory containing the YAML files.

  Returns:
  - merge_defs (dict): A dictionary containing the merged contents of all YAML files in the specified directory.
  """
  merge_defs = dict()
  for file_path in glob.glob(path + '/*'):
    with open(file_path) as file:
      defs = yaml.load(file, Loader=yaml.Loader)
    merge_defs = {**merge_defs, **defs}
  return merge_defs

class Metadata:
  """
  A class that contains metadata information about the codebase.

  Attributes:
  - MetadataRegistry (dict): A dictionary containing metadata information about the codebase.
  - GitUrl (str): The URL of the Git repository.
  - GitCommitId (str): The commit ID of the Git repository.
  - Debug (bool): A flag indicating whether debug mode is enabled.
  - WorkingDir (str): The working directory of the codebase.
  - DataDir (str): The data directory of the codebase.
  - TTL (int): The time-to-live (TTL) value for the metadata.
  - TransformDefs (dict): A dictionary containing the transform definitions.
  - DatasetDefs (dict): A dictionary containing the dataset definitions.
  - StandardAffinities (list): A list of standard affinities.
  """
  MetadataRegistry = None
  GitUrl = None
  GitCommitId = None
  Debug = False
  WorkingDir = None
  DataDir = None
  TTL = 14
  TransformDefs = None
  DatasetDefs = None
  StandardAffinities = ['cpu', 'gpu', 'hive', 'bq']

  @staticmethod
  def setGitUrl(url):
    """
    Sets the Git URL.

    Args:
    - url (str): The URL of the Git repository.
    """
    Metadata.GitUrl = url

  @staticmethod
  def setCommitId(commit_id):
    """
    Sets the Git commit ID.

    Args:
    - commit_id (str): The commit ID of the Git repository.
    """
    Metadata.GitCommitId = commit_id

  @staticmethod
  def setWorkingDir(working_dir):
    """
    Sets the working directory.

    Args:
    - working_dir (str): The working directory of the codebase.
    """
    Metadata.WorkingDir = working_dir
  
  @staticmethod
  def setDataDir(data_dir):
    """
    Sets the data directory.

    Args:
    - data_dir (str): The data directory of the codebase.
    """
    Metadata.DataDir = data_dir

  @staticmethod
  def setTTL(ttl):
    """
    Sets the TTL value.

    Args:
    - ttl (int): The time-to-live (TTL) value for the metadata.
    """
    Metadata.TTL = ttl

  @staticmethod
  def loadYAMLs(specs_path):
    """
    Loads the YAML files.

    Args:
    - specs_path (str): The path to the directory containing the YAML files.
    """
    Metadata.TransformDefs = readYAMLs(os.path.join(specs_path, 'transforms'))
    Metadata.DatasetDefs = readYAMLs(os.path.join(specs_path, 'datasets'))

  @staticmethod
  def setDebug():
    """
    Enables debug mode.
    """
    Metadata.Debug = True

  @staticmethod
  def processMetadata(impl_path, debug=None):
    """
    Processes the metadata.

    Args:
    - impl_path (str): The path to the implementation directory.
    - debug (str): The debug mode to use.

    Returns:
    - dict: A dictionary containing the metadata information.
    """
    metadata = {
      'py': Metadata.getMetadata('py', impl_path),
      'r': Metadata.getMetadata('R', impl_path),
      'hive': {
        'NA': {
          'name': 'NA',
          'type': 'hive',
          'affinity': 'hive',
          'version': '0.0.1'
        }
      },
      'bq': {
        'NA': {
          'name': 'NA',
          'type': 'bq',
          'affinity': 'bq',
          'version': '0.0.1'
        }
      }
    }

    is_valid = True

    for key in metadata.keys():
      if key in ('hive', 'bq'):
        continue
      if Metadata.runValidations(metadata[key]) is False:
        log.critical(key + ' files metadata is not valid')
        is_valid = False

    if is_valid:
      if debug is not None:
        for lang in ['py', 'r']:
          for class_name, class_meta in metadata[lang].items():
            class_meta['affinity'] = debug
            metadata[lang][class_name] = class_meta
      Metadata.MetadataRegistry = metadata
    else:
      Metadata.MetadataRegistry = None

  @staticmethod
  def getMetadata(ext, impl_path):
    """
    Gets the metadata.

    Args:
    - ext (str): The file extension.
    - impl_path (str): The path to the implementation directory.

    Returns:
    - dict: A dictionary containing the metadata information.
    """
    files = [file for file in
             glob.glob(str(os.path.join(impl_path, '**/*.')) + ext, recursive=True)
             if '__' not in file]

    mdata = dict()

    for file in files:
      with open(file, 'r') as f:
        content = f.readlines()
      docstring_flag = False
      meta_str = ''
      for line in content:
        if docstring_flag is not True:
          if '# %%%' not in line:
            continue
          docstring_flag = True
          continue
        if '# %%%' in line:
          break
        meta_str += line.lstrip('#').replace('\t', '  ')
      if docstring_flag:
        meta = yaml.load(meta_str, Loader=yaml.Loader)
        if type(meta) is dict and meta.get('name', None) is not None:
          mdata[meta['name']] = meta
      else:
        # log.warning(file + ' does not contain metadata, please add metadata')
        pass

    return mdata

  @staticmethod
  def runValidations(meta):
    """
    Runs validations on the metadata.

    Args:
    - meta (dict): A dictionary containing the metadata information.

    Returns:
    - bool: True if the metadata is valid, False otherwise.
    """
    is_valid = True
    for t_name, t_meta in meta.items():
      if 'name' not in t_meta.keys():
        log.critical('name not found in ' + t_name)
        is_valid = False
        continue
      if 'type' not in t_meta.keys():
        log.critical('type not found in ' + t_name)
        is_valid = False
        continue
      if 'affinity' not in t_meta.keys():
        log.critical('affinity not found in ' + t_name)
        is_valid = False
        continue
      if 'version' not in t_meta.keys():
        log.critical('version not found in ' + t_name)
        is_valid = False
        continue
      if 'inputs' not in t_meta.keys():
        log.critical('inputs not found in ' + t_name)
        is_valid = False
        continue
      if 'outputs' not in t_meta.keys():
        log.critical('outputs not found in ' + t_name)
        is_valid = False
        continue
      t_type = t_meta['type']
      t_def = Metadata.TransformDefs.get(t_type, None)
      if t_def is None:
        log.critical(t_type + ' does not exist in transform specs')
        is_valid = False
        continue
      for keyword in ['inputs', 'outputs']:
        if t_def.get(keyword, None) is not None:
          for input_name, input_dataset in t_def[keyword].items():
            if t_meta.get(keyword, None) is None:
              is_valid = False
              log.critical(keyword + ' is None in ' + t_name + ' meta')
              continue
            if input_dataset in Metadata.DatasetDefs.keys():
              for input_var, input_type in Metadata.DatasetDefs[input_dataset].items():
                if input_type == 'df':
                  if input_name not in t_meta[keyword].keys():
                    log.critical(input_name + ' not found in inputs in metadata for ' + t_name)
                    is_valid = False
                    continue
                  if input_var not in t_meta[keyword][input_name].keys():
                    log.critical(input_var + ' not in ' + input_name + ' for metadata in ' + t_name)
                    is_valid = False
                    continue
                  try:
                    cols = t_meta[keyword][input_name][input_var]
                  except Exception as err:
                    cols = None
                    log.critical(err)
                  if cols is None:
                    # TODO: Not a rigid check but raises warnings
                    # is_valid = True
                    # log.warning('cols for inputs ' + input_name + ' ' + input_var + ' not defined')
                    pass
            else:
              log.critical(input_dataset + ' missing in dataset specs')
              is_valid = False
              continue
      affinity = t_meta['affinity']
      if affinity not in Metadata.StandardAffinities:
        # Raising a warning here for easy debug. If affinity is incorrect,the task does not run with no other log trace.
        log.warning('For task ' + t_name + ', affinity = ' + affinity + ' is not one of standard affinities - '
                    + ','.join(Metadata.StandardAffinities) + '. Do check if this is what you intend.')

    return is_valid

  @staticmethod
  def generateMetadataString(t_type):
    """
    Generates metadata string for a given transform type.

    Args:
    - t_type (str): The transform type.

    Returns:
    - meta_str (str): The metadata string.
    """
    meta_lines = []
    docstring_mark = '%%%'
    metadata_keys = ['name', 'type', 'affinity', 'version', 'inputs', 'outputs', 'desc']

    meta_lines.append(docstring_mark)
    for key in metadata_keys:
      if key == 'type':
        meta_lines.append(key + ': ' + t_type)
        continue
      if key in ['inputs', 'outputs']:
        meta_lines.append(key + ': ')
        if Metadata.TransformDefs[t_type][key] is not None:
          for dataset_var, dataset_type in Metadata.TransformDefs[t_type][key].items():
            meta_lines.append('  ' + dataset_var + ': ')
            for var, var_type in Metadata.DatasetDefs[dataset_type].items():
              if var_type == 'df':
                meta_lines.append('    ' + var + ': ')
        continue
      meta_lines.append(key + ': ')
    meta_lines.append(docstring_mark)
    for i, line in enumerate(meta_lines):
      meta_lines[i] = '# ' + meta_lines[i]
    meta_str = '\n'.join(meta_lines)
    print(meta_str)


if __name__ == '__main__':
  # Create an argument parser for the metadata utility
  arg_parser = argparse.ArgumentParser(description='metadata utility')

  # Add arguments to the argument parser
  arg_parser.add_argument('-m', '--metadata', action='store', help='Generate Metadata for given Transform type')
  arg_parser.add_argument('-r', '--run', action='store_true', help='Run Validations for written metadata')

  # Parse the arguments
  args = arg_parser.parse_args()

  # If the 'run' argument is provided, process the metadata and check if it is valid
  if args.run:
    Metadata.processMetadata()
    if Metadata.MetadataRegistry is not None:
      log.info('Metadata is valid')
    else:
      log.critical('Metadata is invalid, please check above for files causing error')

  # If the 'metadata' argument is provided, generate metadata string for the given transform type
  if args.metadata is not None:
    Metadata.generateMetadataString(args.metadata)
