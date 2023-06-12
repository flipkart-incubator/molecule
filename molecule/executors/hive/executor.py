import os
import sys
import argparse
import subprocess
import logging
import tempfile
import shutil
import time
sys.path.append(os.getcwd())
from molecule.gsutil_helper import gsutil_helper
from molecule.database import Database

log = logging

def main(args):
  t_hash = args.t_hash
  data_path = os.path.join(args.store_loc, 'data')
  ttl = args.time_to_live

  db = Database(db_env=args.dev_env)
  t_hash_dict = db.loadTransform(t_hash)
  remote_loc = t_hash_dict['data_dir']

  user_related_dir_string = remote_loc.split('users/')[1]
  user_name = user_related_dir_string.split('/')[0]
  log = logger.getLogger(args.t_hash, user=user_name, deployment_type=args.dev_env)

  o_name, o_hash = list(t_hash_dict['outputs'].items())[0]
  hash_dir = os.path.join(data_path, o_hash)
  if not os.path.exists(hash_dir):
    os.makedirs(hash_dir)

  write_lock = os.path.join(hash_dir, 'write.lock')
  open(write_lock, 'a').close()

  params = t_hash_dict['params']

  hql_path = params['hql_path']
  with open(hql_path, 'r') as file:
    query = file.readlines()

  query = ' '.join(query)
  query = ' '.join(query.split('\n'))
  query = ' '.join(query.split('\n'))
  query = query.format(**params)
  
  if args.storage_type == 'gcs':
    output_file = os.path.abspath(os.path.join(hash_dir, 'hive_raw.csv.ttl' + str(ttl)))
  else:
    output_file = os.path.abspath(os.path.join(hash_dir, 'hive_raw.csv'))

  exit_code = 0
  
  log.debug(query)
  qf = tempfile.NamedTemporaryFile(suffix='.hql')
  qf.write(bytes(query, encoding='utf-8'))
  qf.flush()
  # output_file = os.path.abspath(os.path.join(hash_dir, 'hive_raw.csv'))
  sed_regex = '1s/null//g;${/null/d};s/\\r/\\n/g'
  bash_cmd = 'beeline -u "jdbc:hive2://<CONNECTION_STRING>/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" ' \
      + '--hiveconf tez.queue.name=ds --hiveconf hive.resultset.use.unique.column.names=false ' \
      + '-f "{query_file}" --showHeader=true --outputformat=csv2 --silent=true | sed \'{regex}\' | sed \'/^\\s*$/d\' > ' \
      + output_file
  bash_cmd = bash_cmd.format(query_file=qf.name, regex=sed_regex)
  log.debug(bash_cmd)
  
  connected = False
  retries = 0
  process = None
  stderr_dump = ''
  while not connected and retries<5:
    process = subprocess.run(bash_cmd, shell=True, stderr=subprocess.PIPE)
    stderr_dump = process.stderr.decode('utf-8')
    if 'no current connection' in process.stderr.decode('utf-8').lower():
      retries += 1
      time.sleep(60)
    else:
      connected = True

  qf.close()

  if exit_code == 0 and (process.returncode != 0 or os.path.getsize(output_file) < 10):
    log.critical('***** STDERR *****')
    log.critical(stderr_dump)
    exit_code = 1

  if exit_code == 0:
    log.info('hive pull completed!')
    os.remove(write_lock)
    if args.time_to_live is not None and args.storage_type != 'gcs':
      ttl_loc = os.path.join(hash_dir, 'del_' + str(args.time_to_live) + '.ttl')
      open(ttl_loc, 'a').close()
    os.chmod(hash_dir, 0o777)
    if args.storage_type == 'gcs':
      log.info('uploading to gcs')
      if not gsutil_helper(hash_dir, remote_loc, mode='write'):
        shutil.rmtree(hash_dir)
        raise Exception('gcs upload failed')
  else:
    shutil.rmtree(hash_dir)
    log.critical('hive pull failed!')

  exit(exit_code)


if __name__ == '__main__':
  sys.path.append(os.getcwd())
  import molecule.executors.py.logger as logger

  arg_parser = argparse.ArgumentParser(description='spawner for hive-based transforms')
  arg_parser.add_argument('-t', '--t-hash', action='store', help='Transform Hash')
  arg_parser.add_argument('-s', '--store-loc', action='store', help='Store location for files')
  arg_parser.add_argument('-wd', '--working-dir', action='store', help='Working dir for files')
  arg_parser.add_argument('-ttl', '--time-to-live', action='store', help='TTL for files in days')
  arg_parser.add_argument('--storage-type', action='store', help='nfs or gcs storage')
  arg_parser.add_argument('--dev-env', action='store', help='stage or prod env')

  cmd_args = arg_parser.parse_args()

  if cmd_args.storage_type is None:
    cmd_args.storage_type = 'nfs'
  if cmd_args.time_to_live is None or cmd_args.time_to_live == 'None':
    cmd_args.time_to_live = 120
  else:
    cmd_args.time_to_live = int(cmd_args.time_to_live)

  main(cmd_args)
