import os
import sys
import argparse
import logging
import shutil
import yaml

from google.cloud import bigquery
from time import sleep

sys.path.append(os.getcwd())
from molecule.gsutil_helper import gsutil_helper
from molecule.database import Database

log = logging

def main(args):
  t_hash = args.t_hash
  data_path = os.path.join(args.store_loc, 'data')

  db = Database(db_env=args.dev_env)
  t_hash_dict = db.loadTransform(t_hash)
  remote_loc = t_hash_dict['data_dir']

  user_related_dir_string = remote_loc.split('users/')[1]
  user_name = user_related_dir_string.split('/')[0]
  log = logger.getLogger(args.t_hash, user=user_name, deployment_type=args.dev_env)

  log.info(t_hash_dict)
  o_name, o_hash = list(t_hash_dict['outputs'].items())[0]

  hash_dir = os.path.join(data_path, o_hash)
  write_lock = os.path.join(hash_dir, 'write.lock')

  if os.path.exists(hash_dir) and os.path.exists(write_lock):
    while os.path.exists(write_lock):
      sleep(1)

  if os.path.exists(hash_dir) and not os.path.exists(write_lock):
    log.info('skipping as data already exists')
    exit(0)

  if not os.path.exists(hash_dir):
    os.makedirs(hash_dir)

  open(write_lock, 'a').close()

  params = t_hash_dict['params']

  hql_path = params['hql_path']
  with open(hql_path, 'r') as file:
    query = file.readlines()

  query = ' '.join(query)
  query = ' '.join(query.split('\n'))
  query = ' '.join(query.split('\n'))
  query = query.format(**params)

  log.debug(query)
  output_file = os.path.abspath(os.path.join(hash_dir, 'hive_raw.csv'))

  bqclient = bigquery.Client(
      project=args.gcp_config['GOOGLE_CLOUD_PROJECT'], location=args.gcp_config['REGION'])
  job = bqclient.query(query, retry=bigquery.DEFAULT_RETRY)

  while job.running():
    sleep(1)
    log.info('Running query...')

  print('Job completed in ', job.ended - job.started)

  df = job.to_dataframe()

  df.to_csv(output_file, index=False)

  exit_code = -1
  if job.done():
    exit_code = 0
  else:
    exit_code = 1

  if exit_code == 0:
    log.info('data pull completed!')
    os.remove(write_lock)
    if args.storage_type == 'gcs':
      log.info('uploading to gcs')
      if not gsutil_helper(hash_dir, mode='write'):
        shutil.rmtree(hash_dir)
        raise Exception('gcs upload failed')
  else:
    shutil.rmtree(hash_dir)
    log.critical('data pull failed!')

  exit(exit_code)


if __name__ == '__main__':
  sys.path.append(os.getcwd())
  import molecule.executors.py.logger as logger

  arg_parser = argparse.ArgumentParser(description='spawner for hive-based transforms')
  arg_parser.add_argument('-t', '--t-hash', action='store', help='Transform Hash')
  arg_parser.add_argument('-s', '--store-loc', action='store', help='Store location for files')
  arg_parser.add_argument('-gcp', '--gcp-config', action='store', help='GCP related details')
  arg_parser.add_argument('-ttl', '--time-to-live', action='store', help='TTL for files in days')
  arg_parser.add_argument('--storage-type', action='store', help='nfs or gcs storage')
  arg_parser.add_argument('--dev-env', action='store', help='stage or prod env')

  cmd_args = arg_parser.parse_args()

  if cmd_args.storage_type is None:
    cmd_args.storage_type = 'nfs'
  if cmd_args.time_to_live is None or cmd_args.time_to_live == 'None':
    cmd_args.time_to_live = None
  else:
    cmd_args.time_to_live = int(cmd_args.time_to_live)

  if cmd_args.gcp_config is None or cmd_args.gcp_config == 'None':
    cmd_args.gcp_config = {
        'GOOGLE_CLOUD_PROJECT': os.environ['PEOJECT_ID'],
        'REGION': 'asia-south1'
    }
  else:
    cmd_args.gcp_config = yaml.safe_load(cmd_args.gcp_config)

  main(cmd_args)