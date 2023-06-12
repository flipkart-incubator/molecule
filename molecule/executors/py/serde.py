import os
import yaml
import pandas as pd
import pickle

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
  def write(file_path, blob, file_type, ttl=None):
    if not os.path.exists(file_path):
      os.makedirs(file_path)

    if ttl is not None:
      ttl_loc = os.path.join(file_path, 'del_' + str(ttl) + '.ttl')
      open(ttl_loc, 'a').close()

    kv_pairs = {}
    for var_name, var_type in SerDe.data_defs[file_type].items():
      if var_type == 'df':
        write_df(file_path, getattr(blob, var_name), var_name)
      # TODO: Handle dates explicitly
      elif var_type == 'pickle':
        write_pickle(file_path, getattr(blob, var_name), var_name)
      else:
        kv_pairs[var_name] = getattr(blob, var_name)
    if len(kv_pairs) != 0:
      write_yaml(file_path, kv_pairs)

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
  def read(file_path, file_type):
    blob = Dataset()
    kv_pairs = []
    kv_file = None

    for var_name, var_type in SerDe.data_defs[file_type].items():
      if var_type == 'df':
        setattr(blob, var_name, read_df(file_path, var_name))
      # TODO: Handle dates explicitly
      elif var_type == 'pickle':
        setattr(blob, var_name, read_pickle(file_path, var_name))
      else:
        if kv_file is None:
          kv_file = read_yaml(file_path)
        kv_pairs.append(var_name)

    for key in kv_pairs:
      setattr(blob, key, kv_file[key])

    return blob


def write_df(file_path, blob: pd.DataFrame, file_name):
  # loc = os.path.join(file_path, file_name+'.feather')
  # blob.to_feather(loc)
  loc = os.path.join(file_path, file_name+'.csv')
  write_lock = os.path.join(file_path, 'write.lock')
  open(write_lock, 'a').close()
  blob.to_csv(loc, index=False)
  os.chmod(loc, 0o777)
  os.remove(write_lock)


def read_df(file_path, file_name):

  # loc = os.path.join(file_path, file_name+'.feather')
  # if os.path.isfile(loc):
  #   blob = pd.read_feather(loc)
  #   return blob

  loc = os.path.join(file_path, file_name+'.csv')
  if os.path.isfile(loc):
    blob = pd.read_csv(loc, low_memory=False)
    return blob

  # loc = os.path.join(file_path, file_name+'.rds')
  # if os.path.isfile(loc):
  #   blob = pyreadr.read_r(loc)[None]
  #   return blob

  loc = os.path.join(file_path, file_name+'.tsv')
  if os.path.isfile(loc):
    blob = pd.read_csv(loc, sep='\t', low_memory=False)
    return blob


def write_yaml(file_path, blob):
  loc = os.path.join(file_path, 'key_value.yaml')
  write_lock = os.path.join(file_path, 'write.lock')
  open(write_lock, 'a').close()
  with open(loc, 'w') as file:
    yaml.dump(blob, file)
  os.chmod(loc, 0o777)
  os.remove(write_lock)


def read_yaml(file_path):
  loc = os.path.join(file_path, 'key_value.yaml')
  if os.path.isfile(loc):
    with open(loc, 'r') as file:
      blob = yaml.load(file, Loader=yaml.Loader)
    return blob
  else:
    raise Exception("file not found: %s", loc)


def write_pickle(file_path, blob, file_name):
  loc = os.path.join(file_path, file_name+'.pickle')
  write_lock = os.path.join(file_path, 'write.lock')
  open(write_lock, 'a').close()
  with open(loc, 'wb') as f:
    pickle.dump(blob, f)
  os.chmod(loc, 0o777)
  os.remove(write_lock)


def read_pickle(file_path, file_name):
  loc = os.path.join(file_path, file_name+'.pickle')
  if os.path.isfile(loc):
    with open(loc, 'rb') as f:
      blob = pickle.load(f)
    return blob
  else:
    raise Exception("file not found: %s", loc)

