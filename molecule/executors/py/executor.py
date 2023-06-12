import os
import importlib
import logging
import argparse
import storage
import logger
import sys
sys.path.append(os.getcwd())
from molecule.database import Database



plog = logging

class Dataset:
  def __init__(self, **kwargs):
    self.__dict__.update(kwargs)


def get_py_files(src):
  cwd = os.getcwd()  # Current Working directory
  py_files = []
  for root, dirs, files in os.walk(src):
    for file in files:
      if file.endswith(".py"):
        py_files.append(os.path.join(cwd, root, file))
  py_files = [path for path in py_files if 'legacy' not in path]
  return py_files


def dynamic_import(module_name, py_path):
  module_spec = importlib.util.spec_from_file_location(module_name, py_path)
  module = importlib.util.module_from_spec(module_spec)
  module_spec.loader.exec_module(module)
  return module


def dynamic_import_from_src(src, log=plog, star_import=False):
  my_py_files = get_py_files(src)
  for py_file in my_py_files:
    try:
      module_name = os.path.split(py_file)[-1].strip(".py")
      imported_module = dynamic_import(module_name, py_file)
      if star_import:
        for obj in dir(imported_module):
          globals()[obj] = imported_module.__dict__[obj]
      else:
        globals()[module_name] = imported_module
    except Exception as err:
      log.warning('failed to import module ' + py_file)
      log.warning(err)
  return

def importAll(wd, logger=plog):
  loc = os.path.join(wd, 'impl/pycode')
  dynamic_import_from_src(loc, log=logger, star_import=True)

def initStore(wd, store_loc, storage_type='nfs', db=None, remote_loc=None):  
  store = storage.Storage(store_loc, defs_path=os.path.join(wd, 'specs/datasets/'), 
                          mode=storage_type, db=db, remote_loc=remote_loc)
  return store

def loadPaths(store, t_hash_dict, io='inputs'):
  paths = {}

  io_type = t_hash_dict[str(io)+'_type']

  for io_name, io_hash in t_hash_dict[io].items():
    if type(io_hash) is dict:
      paths[io_name] = {}
      for in_ser, in_hash in io_hash.items():
        paths[io_name][in_ser] = store.getPaths(in_hash, io_type[io_name])
    else:
      paths[io_name] = store.getPaths(io_hash, io_type[io_name])

  return paths

def loadInputs(store, t_hash_dict):
  inputs = {}

  inputs_type = t_hash_dict['inputs_type']
  params = t_hash_dict['params']

  for input_name, input_hash in t_hash_dict['inputs'].items():
    if type(input_hash) is dict:
      inputs[input_name] = {}
      for in_ser, in_hash in input_hash.items():
        inputs[input_name][in_ser] = store.load(in_hash, inputs_type[input_name])
    else:
      inputs[input_name] = store.load(input_hash, inputs_type[input_name])

  return inputs, params

def loadOutputs(store, t_hash_dict):
  outputs = {}

  outputs_type = t_hash_dict['outputs_type']

  for output_name, output_hash in t_hash_dict['outputs'].items():
    if type(output_hash) is dict:
      outputs[output_name] = {}
      for in_ser, in_hash in output_hash.items():
        outputs[output_name][in_ser] = store.load(in_hash, outputs_type[output_name])
    else:
      outputs[output_name] = store.load(output_hash, outputs_type[output_name])

  return outputs

def getInstance(t_hash_dict, inputs, params, context=None):
  t_class_name = t_hash_dict['transform']['transform_class']
  t_cls = eval(t_class_name)
  ti = t_cls(inputs, params, context=context)

  return ti

def saveOutputs(store, t_hash_dict, ti):
  for output_name, output_hash in t_hash_dict['outputs'].items():
    store.save(output_hash, ti.outputs[output_name])

def saveOutputsToFile(store, t_hash_dict, ti, ttl=28):
  outputs_type = t_hash_dict['outputs_type']

  for output_name, output_hash in t_hash_dict['outputs'].items():
    store.saveToFile(output_hash, ti.outputs[output_name], outputs_type[output_name], ttl)

def deleteHashes(store, t_hash_dict, ti):
  if ti.outputs.get('delete_hashes', None) is not None and len(ti.outputs['delete_hashes']) > 0:
    for inp_name in ti.outputs['delete_hashes']:
      data_hash = t_hash_dict['inputs'][inp_name]
      store.delete(data_hash)
    exit(1)

def main():
  # parse args
  arg_parser = argparse.ArgumentParser(description='spawner for python-based transforms')
  arg_parser.add_argument('-t', '--t-hash', action='store', help='Transform Hash')
  arg_parser.add_argument('-s', '--store-loc', action='store', help='Store location for files')
  arg_parser.add_argument('-wd', '--working-dir', action='store', help='Working dir for files')
  arg_parser.add_argument('-ttl', '--time-to-live', action='store', help='TTL for files in days')
  arg_parser.add_argument('--storage-type', action='store', help='nfs or gcs storage')
  arg_parser.add_argument('--dev-env', action='store', help='stage or prod env')

  args = arg_parser.parse_args()

  if args.time_to_live is None or args.time_to_live == 'None':
    args.time_to_live = None
  else:
    args.time_to_live = int(args.time_to_live)

  ## Connect to database
  db = Database(db_env=args.dev_env)
  t_hash_dict = db.loadTransform(args.t_hash)
  # init store object
  user_related_dir_string = t_hash_dict['data_dir'].split('users/')[1]
  user_name = user_related_dir_string.split('/')[0]
  log = logger.getLogger(args.t_hash, user=user_name, deployment_type=args.dev_env)
  log.info('Initializing Dict')

  store = initStore(args.working_dir, args.store_loc, args.storage_type, db, remote_loc=t_hash_dict['data_dir'])

  log.info('Importing Classes')
  # do imports
  sys.path.append(os.path.join(args.working_dir, 'impl/pycode/'))
  importAll(args.working_dir, logger=log)
  log.info('Loading Inputs')
  inputs, params = loadInputs(store, t_hash_dict)
  inputs_path = loadPaths(store, t_hash_dict)
  outputs_path = loadPaths(store, t_hash_dict, 'outputs')
  log.info('Running Transform')
  context = Dataset(logger=log, paths=(inputs_path, outputs_path))
  ti = getInstance(t_hash_dict, inputs, params, context=context)
  ti.run()
  log.info('Checking for delete_hashes')
  deleteHashes(store, t_hash_dict, ti)
  log.info('Saving Outputs')
  saveOutputsToFile(store, t_hash_dict, ti, args.time_to_live)
  db.close()
  exit(0)


if __name__ == '__main__':
  sys.path.append(os.getcwd())
  import molecule.pl_logger as pl_logger

  plog = pl_logger.getLogger('py_executor')

  main()
