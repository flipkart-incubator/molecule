import os
import yaml
import pandas as pd
import pickle
import shutil
import sys
sys.path.append(os.getcwd())
from molecule.gsutil_helper import gsutil_helper

class Dataset:
  pass

class SerDe:
  data_defs = {}

  def __init__(self, defs_path):
    data_def_files = os.listdir(defs_path)
    for def_file_name in data_def_files:
      def_path = os.path.join(defs_path, def_file_name)
      with open(def_path, 'r') as def_file:
        defs = yaml.load(def_file, Loader=yaml.Loader)
      SerDe.data_defs = {**SerDe.data_defs, **defs}

  @staticmethod
  def write(file_path, blob, file_type, remote_path, ttl=14):
    if not os.path.exists(file_path):
      os.makedirs(file_path)

    # if ttl is not None:
    ttl_loc = os.path.join(file_path, 'del_' + str(ttl) + '.ttl')
    open(ttl_loc, 'a').close()

    kv_pairs = {}
    for var_name, var_type in SerDe.data_defs[file_type].items():
      if var_type == 'df':
        write_df(file_path, getattr(blob, var_name), var_name, ttl)
      # TODO: Handle dates explicitly
      elif var_type == 'pickle':
        write_pickle(file_path, getattr(blob, var_name), var_name, ttl)
      else:
        kv_pairs[var_name] = getattr(blob, var_name)
    if len(kv_pairs) != 0:
      write_yaml(file_path, kv_pairs, ttl)

    if not gsutil_helper(file_path, remote_path, mode='write'):
      shutil.rmtree(file_path)
      raise Exception('gcs copy failed')
    shutil.rmtree(file_path)

  @staticmethod
  def get_paths(file_path, file_type):
    blob = Dataset()
    kv_pairs = []
    kv_file = None
    files = None
    if os.path.exists(file_path):
      files = os.listdir(file_path)

    for var_name, var_type in SerDe.data_defs[file_type].items():
      if var_type in ['df', 'pickle']:
        if files is not None:
          file_loc = os.path.join(file_path, [fn for fn in files if var_name in fn][0])
        else:
          if var_type == 'df':
            file_loc = os.path.join(file_path, var_name+'.csv')
          else:
            file_loc = os.path.join(file_path, var_name+'.'+var_type)
        setattr(blob, var_name, file_loc)
      else:
        if kv_file is None:
          kv_file = os.path.join(file_path, 'key_value.yaml')
        kv_pairs.append(var_name)

    for key in kv_pairs:
      setattr(blob, key, kv_file)

    return blob

  @staticmethod
  def read(file_path, file_type, remote_path, ttl=14):
    if not os.path.exists(file_path) and not gsutil_helper(file_path, remote_path, mode='read'):
      shutil.rmtree(file_path)
      raise Exception('gcs copy failed')

    blob = Dataset()
    kv_pairs = []
    kv_file = None

    for var_name, var_type in SerDe.data_defs[file_type].items():
      if var_type == 'df':
        setattr(blob, var_name, read_df(file_path, var_name, ttl))
      # TODO: Handle dates explicitly
      elif var_type == 'pickle':
        setattr(blob, var_name, read_pickle(file_path, var_name, ttl))
      else:
        if kv_file is None:
          kv_file = read_yaml(file_path, ttl)
        kv_pairs.append(var_name)

    for key in kv_pairs:
      setattr(blob, key, kv_file[key])

    shutil.rmtree(file_path)
    return blob


def write_df(file_path, blob: pd.DataFrame, file_name, ttl=14):
  # loc = os.path.join(file_path, file_name+'.feather')
  # blob.to_feather(loc)
  loc = os.path.join(file_path, file_name+'.csv.ttl' + str(ttl))
  # write_lock = os.path.join(file_path, 'write.lock')
  # open(write_lock, 'a').close()
  blob.to_csv(loc, index=False)
  os.chmod(loc, 0o777)
  # os.remove(write_lock)


def read_df(file_path, file_name, ttl=14):

  # loc = os.path.join(file_path, file_name+'.feather')
  # if os.path.isfile(loc):
  #   blob = pd.read_feather(loc)
  #   return blob

  loc = os.path.join(file_path, file_name+'.csv.ttl' + str(ttl))
  if os.path.isfile(loc):
    blob = pd.read_csv(loc, low_memory=False)
    return blob

  # loc = os.path.join(file_path, file_name+'.rds')
  # if os.path.isfile(loc):
  #   blob = pyreadr.read_r(loc)[None]
  #   return blob

  loc = os.path.join(file_path, file_name+'.tsv.ttl' + str(ttl))
  if os.path.isfile(loc):
    blob = pd.read_csv(loc, sep='\t', low_memory=False)
    return blob


def write_yaml(file_path, blob, ttl=14):
  loc = os.path.join(file_path, 'key_value.yaml.ttl' + str(ttl))
  # write_lock = os.path.join(file_path, 'write.lock')
  # open(write_lock, 'a').close()
  with open(loc, 'w') as file:
    yaml.dump(blob, file)
  os.chmod(loc, 0o777)
  # os.remove(write_lock)


def read_yaml(file_path, ttl=14):
  loc = os.path.join(file_path, 'key_value.yaml.ttl' + str(ttl))
  if os.path.isfile(loc):
    with open(loc, 'r') as file:
      blob = yaml.load(file, Loader=yaml.Loader)
    return blob
  else:
    raise Exception("file not found: %s", loc)


def write_pickle(file_path, blob, file_name, ttl=14):
  loc = os.path.join(file_path, file_name+'.pickle.ttl' + str(ttl))
  # write_lock = os.path.join(file_path, 'write.lock')
  # open(write_lock, 'a').close()
  with open(loc, 'wb') as f:
    pickle.dump(blob, f)
  os.chmod(loc, 0o777)
  # os.remove(write_lock)


def read_pickle(file_path, file_name, ttl=14):
  loc = os.path.join(file_path, file_name+ '.pickle.ttl' + str(ttl))
  if os.path.isfile(loc):
    with open(loc, 'rb') as f:
      blob = pickle.load(f)
    return blob
  else:
    raise Exception("file not found: %s", loc)

