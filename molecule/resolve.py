import os
import subprocess
import tarfile
from getpass import getuser
from google.cloud import storage
from tempfile import NamedTemporaryFile

from .metadata import Metadata


class Resolve:
  """
  A class that provides methods to resolve paths, working directories, git repositories, metadata, and code synchronization.
  """
  wd = ''
  projects_path = ''
  specs_path = ''
  operators_path = ''
  impl_path = ''

  @staticmethod
  def updatePaths(wd):
    """
    A static method that updates the paths of the working directory, projects, specifications, operators, and implementation.

    Args:
    - wd: A string representing the working directory path.
    """
    Resolve.wd = wd
    Resolve.projects_path = Resolve.resolvePath('projects')
    Resolve.specs_path = Resolve.resolvePath('specs')
    Resolve.operators_path = Resolve.resolvePath('operators')
    Resolve.impl_path = Resolve.resolvePath('impl')

  @staticmethod
  def resolvePath(path):
    """
    A static method that resolves the path of a given directory.

    Args:
    - path: A string representing the directory path.

    Returns:
    - A string representing the resolved path.
    """
    return os.path.join(Resolve.wd, path)

  @staticmethod
  def resolveWorkingDirectory(cfg, store):
    """
    A static method that resolves the working directory path.

    Args:
    - cfg: A dictionary representing the pipeline configuration.
    - store: An object representing the storage.

    Returns:
    - A string representing the resolved working directory path.
    """
    if cfg.get('git_url', None) is None and cfg.get('local_path', None) is not None:
      wd = cfg['local_path']
    elif cfg.get('git_url', None) is not None:
      wd = store.gitClone(Metadata.GitUrl, Metadata.GitCommitId)
    else:
      raise Exception('cannot resolve working directory')
    Metadata.setWorkingDir(wd)
    Resolve.updatePaths(wd)
    return wd
  
  @staticmethod
  def get_remote_path(prod=False):
    """
    A static method that gets the remote path.

    Args:
    - prod: A boolean representing whether the remote path is for production or not.

    Returns:
    - A string representing the remote path.
    """
    if not prod:
      curr_user = getuser()
      data_dir = os.path.join('gs://', os.environ['MOLECULE_BUCKET_NAME'], 'users', curr_user, 'store', 'data')
    else:
      data_dir = os.path.join('gs://', os.environ['MOLECULE_BUCKET_NAME'], 'users', 'prod', 'store', 'data')
    return data_dir

  @staticmethod
  def get_working_dir(p_name, mode='local'):
    """
    A static method that gets the working directory path.

    Args:
    - p_name: A string representing the project name.
    - mode: A string representing the mode.

    Returns:
    - A string representing the working directory path.
    """
    curr_user = getuser()
    if mode == 'local':
      blob_loc = os.path.join('/tmp/', curr_user, 'molecule_{p_name}.tar'.format(p_name=p_name))
    elif mode == 'gcp':
      blob_loc = os.path.join('users', curr_user,
                              'molecule_{p_name}.tar'.format(p_name=p_name))
    else:
      raise Exception('invalid mode')
    return blob_loc

  @staticmethod
  def resolveGit(pipeline_config):
    """
    A static method that resolves the git repository.

    Args:
    - pipeline_config: A dictionary representing the pipeline configuration.
    """
    if pipeline_config.get('git_url', None) is not None:
      Metadata.setGitUrl(pipeline_config['git_url'])
      Metadata.setDataDir(Resolve.get_remote_path(prod=True))
      if pipeline_config.get('git_commit_id', None) is not None:
        Metadata.setCommitId(pipeline_config['git_commit_id'])
      elif pipeline_config.get('git_branch', None) is not None:
        git_cmd = 'git ls-remote {url} refs/head/{branch}'.format(url=pipeline_config['git_url'], branch=pipeline_config['git_branch'])
        process = subprocess.run(git_cmd, shell=True, stdout=subprocess.PIPE)
        if process.returncode != 0:
          raise Exception('cannot access remote git')
        cid = process.stdout.decode('utf-8').split('\t')[0]
        Metadata.setCommitId(cid)
      else:
        raise Exception("Insufficient Config")
    else:
      Metadata.setDataDir(Resolve.get_remote_path())
    print("Setting remote data dir", Metadata.DataDir)

  @staticmethod
  def resolveMeta(debug=None):
    """
    A static method that resolves the metadata.

    Args:
    - debug: A boolean representing whether the debug mode is on or not.
    """
    Metadata.loadYAMLs(Resolve.specs_path)
    Metadata.processMetadata(Resolve.impl_path, debug=debug)
    Metadata.setDebug()

  @staticmethod
  def syncCode(blob_loc, working_dir, mode='local'):
    """
    A static method that synchronizes the code.

    Args:
    - blob_loc: A string representing the blob location.
    - working_dir: A string representing the working directory path.
    - mode: A string representing the mode.
    """
    if mode == 'local':
      with NamedTemporaryFile('wb', suffix='.tar') as f:
        with tarfile.open(fileobj=f, mode='w|') as tar:
          tar.add(working_dir, arcname='code', filter=lambda x: None if '.git/' in x.name else x)
        if not os.path.exists(os.path.dirname(blob_loc)):
          os.makedirs(os.path.dirname(blob_loc))
        subprocess.run(['cp', f.name, blob_loc], check=True)

    elif mode == 'gcp':
      bucket = storage.Client(
          project=os.environ['GOOGLE_CLOUD_PROJECT']).bucket(os.environ['MOLECULE_BUCKET_NAME'])
      blob = bucket.blob(blob_loc)

      with NamedTemporaryFile('wb', suffix='.tar') as f:
        with tarfile.open(fileobj=f, mode='w|') as tar:
          tar.add(working_dir, arcname='code', filter=lambda x: None if '.git/' in x.name else x)
        blob.upload_from_filename(f.name)

      # return os.path.join('gs://molecule-prod-bucket', blob_loc)
    else:
      raise Exception('invalid mode')
