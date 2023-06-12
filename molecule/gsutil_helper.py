import os
import argparse
import subprocess
import sys
import time 

sys.path.append(os.path.dirname(__file__))
from . import pl_logger as logger

try:
  log = logger.getLogger('gsutil_helper', enable_cloud_logging=False)
except NameError:
  pass

def run_cmd(cmd):
  """
  Runs a command in the shell and returns the return code.

  Args:
  - cmd (str): The command to run.

  Returns:
  - int: The return code of the command.
  """
  log.debug(cmd)
  retry = 0
  p = None
  while retry < 5:
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode == 0:
      break
    retry += 1
    time.sleep(30)
  return p.returncode

def gsutil_helper(file_path, remote_loc, mode='check'):
  """
  Helper function to interact with Google Cloud Storage using gsutil.

  Args:
  - file_path (str): The local file path.
  - remote_loc (str): The remote location in GCS.
  - mode (str): The mode of operation. Can be 'check', 'read', 'write', or 'delete'.

  Returns:
  - bool: True if the operation was successful, False otherwise.
  """
  file_path = os.path.normpath(file_path)
  
  hash_dir = file_path.split('/store/data/')[1]
  gcs_path = os.path.join(remote_loc, hash_dir)
  res = None
  if mode == 'read':
    cmd = 'gsutil cp -r {} {}'.format(gcs_path, os.path.dirname(file_path))
  elif mode == 'write':
    cmd = 'gsutil cp -r {} {}'.format(file_path, os.path.dirname(gcs_path) + '/')
  elif mode == 'check':
    cmd = 'gsutil ls {}'.format(gcs_path)
  elif mode == 'delete':
    cmd = 'gsutil rm -rf {}'.format(gcs_path)
  else:
    raise Exception('unrecognized mode')
  res = run_cmd(cmd)

  if res is not None and res == 0:
    return True
  else:
    return False

if __name__=='__main__':
  arg_parser = argparse.ArgumentParser(
      description='gsutil utility')
  arg_parser.add_argument('-fp', '--file_path',
                          action='store', help='File/Dir Path')
  arg_parser.add_argument('-rp', '--remote_file_path',
                          action='store', help='Remote File/Dir Path')
  arg_parser.add_argument('-m', '--mode',
                          action='store', help='Mode check/read/write')
  args = arg_parser.parse_args()
  # logging.disable(logging.CRITICAL)
  # sys.stdout = open(os.devnull, 'w')
  res = gsutil_helper(args.file_path, args.remote_file_path, mode=args.mode)
  # sys.stdout = sys.__stdout__
  exit(int(not res))
