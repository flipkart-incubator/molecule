import os
import serde
import subprocess
import fcntl
import shutil
import gcs_serde
from hashlib import md5
from resources import TaskStatus

request_id = '0000000'
partition_val = None
if 'REQUEST_ID' in os.environ:
  request_id = os.environ['REQUEST_ID']
if 'PARTITION_VAL' in os.environ:
  partition_val = os.environ['PARTITION_VAL']


class Singleton(type):
  _instances = {}

  def __call__(cls, *args, **kwargs):
    if cls not in cls._instances:
      cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
    return cls._instances[cls]


class BaseStorage:
  def __init__(self, store_loc, mkdir=True, mode='nfs'):
    self.mode = mode
    self.store_loc = store_loc
    self.pl_plan_store = os.path.join(self.store_loc, 'pipeline_plans')
    self.data_store = os.path.join(self.store_loc, 'data')

    if mkdir and not os.path.exists(self.store_loc):
      os.makedirs(self.store_loc)
    if mkdir and not os.path.exists(self.pl_plan_store):
      os.makedirs(self.pl_plan_store)
    if self.mode == 'nfs' and mkdir and not os.path.exists(self.data_store):
      os.makedirs(self.data_store)

    self.memory_store = dict()

  def check(self, hash_key):
    raise NotImplementedError('check not implemented in class')

  def load(self, hash_key, blob_type):
    raise NotImplementedError('load not implemented in class')

  def save(self, hash_key, blob):
    raise NotImplementedError('save not implemented in class')

  def saveToFile(self, hash_key, blob, blob_type, ttl=28):
    raise NotImplementedError('saveToFile not implemented in class')

  def delete(self, hash_key):
    raise NotImplementedError('delete not implemented in class')


class Storage(BaseStorage):
  def __init__(self, store_loc, defs_path, mode='nfs', db=None, remote_loc=None):
    super().__init__(store_loc, mode=mode)
    self.db = db
    self.remote_loc = remote_loc
    if self.mode == 'nfs':
      self.serde = serde.SerDe(defs_path)
    if self.mode == 'gcs':
      self.serde = gcs_serde.SerDe(defs_path)

  def check(self, hash_key):
    # FIXME: not actually looking for file, might be an error
    if hash_key in self.memory_store.keys():
      return True
    loc = os.path.join(self.data_store, hash_key)
    write_lock = os.path.join(loc, 'write.lock')
    if self.mode == 'nfs':
      return os.path.exists(loc) and not os.path.exists(write_lock)
    if self.mode == 'gcs':
      if os.path.exists(loc) and not os.path.exists(write_lock):
        return True
      else:
        # Checking in load function to get TTL and returing TRUE here to avoid multiple db calls
        return True

  def getPaths(self, hash_key, blob_type):
    loc = os.path.join(self.data_store, hash_key)
    blob = self.serde.get_paths(loc, blob_type)
    return blob

  def load(self, hash_key, blob_type):
    if self.check(hash_key):
      if hash_key in self.memory_store.keys():
        return self.memory_store[hash_key]
      loc = os.path.join(self.data_store, hash_key)
      if self.mode == 'gcs':
        ds_ttl = self.db.getDatasetTTL(os.path.join(self.remote_loc, hash_key))
        if ds_ttl == -1:
          raise Exception('No such hash in store: ', hash_key)
        else:
          blob = self.serde.read(loc, blob_type, self.remote_loc, ttl=ds_ttl)
      else:
        blob = self.serde.read(loc, blob_type)
      return blob

    raise Exception('No such hash in store: ', hash_key)

  def save(self, hash_key, blob):
    self.memory_store[hash_key] = blob

  def saveToFile(self, hash_key, blob, blob_type, ttl=28):
    self.save(hash_key, blob)
    loc = os.path.join(self.data_store, hash_key)
    ## TODO : Make write definition consistent across storage type
    if self.mode == 'gcs':
      self.serde.write(loc, blob, blob_type, self.remote_loc, ttl=ttl)
      self.db.updateDatasetHash(os.path.join(self.remote_loc, hash_key), TaskStatus.COMPLETED)
    else:
      self.serde.write(loc, blob, blob_type, ttl=ttl)

  def delete(self, hash_key):
    if self.check(hash_key):
      loc = os.path.join(self.data_store, hash_key)
      _ = self.db.deleteDatasetHash(os.path.join(self.remote_loc, hash_key))
      if self.mode == 'nfs':
        shutil.rmtree(loc, ignore_errors=True)
      if self.mode == 'gcs':
        gcs_serde.gsutil_helper(loc, self.remote_loc, mode='delete')


class DebugStorage(BaseStorage):
  def __init__(self, store_loc, remote_ip, remote_store_loc, defs_path, mode='nfs', db=None):
    super().__init__(store_loc, mode=mode)
    self.remote_ip = remote_ip
    self.local_store = Storage(store_loc, defs_path, mode=mode, db=db, remote_loc=remote_store_loc)
    if remote_store_loc is None:
      remote_store_loc = '/tmp'
    if self.remote_ip is None:
      self.remote_store = Storage(remote_store_loc, defs_path, mode=mode)
    else:
      self.remote_store = BaseStorage(remote_store_loc, mkdir=False)

  # def loadTransform(self, ti_hash):
  #   return self.local_store.loadTransform(ti_hash)

  def getPaths(self, hash_key, blob_type):
    self.local_store.getPaths(hash_key, blob_type)

  def remoteCheck(self, remote_loc):
    cmd = 'ssh -o ConnectTimeout=5 ' + self.remote_ip + ' test -e ' + remote_loc
    p = subprocess.run(cmd, shell=True)
    remote_write_lock = os.path.join(remote_loc, 'write.lock')
    cmd = 'ssh -o ConnectTimeout=5 ' + self.remote_ip + ' test -e ' + remote_write_lock
    q = subprocess.run(cmd, shell=True)
    if p.returncode == 0 and q.returncode != 0:
      return True
    else:
      return False

  def remoteCopy(self, remote_loc):
    cmd = 'rsync -e "ssh -o ConnectTimeout=5" --info=progress2 -rzh ' + self.remote_ip + ':' + remote_loc + ' ' + self.local_store.data_store
    p = subprocess.run(cmd, shell=True)
    if p.returncode == 0:
      return True
    else:
      raise Exception('SCP file transfer error')

  def check(self, hash_key):
    if self.local_store.check(hash_key) is True:
      return True
    if self.remote_ip is None:
      return self.remote_store.check(hash_key)
    else:
      remote_loc = os.path.join(self.remote_store.data_store, hash_key)
      return self.remoteCheck(remote_loc)

  def load(self, hash_key, blob_type):
    if self.check(hash_key):
      if self.local_store.check(hash_key):
        f = open('/tmp/'+hash_key, 'w')
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        blob = self.local_store.load(hash_key, blob_type)
        fcntl.lockf(f, fcntl.LOCK_UN)
        return blob
      elif self.remote_ip is None and self.remote_store.check(hash_key):
        f = open('/tmp/'+hash_key, 'w')
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        blob = self.remote_store.load(hash_key, blob_type)
        fcntl.lockf(f, fcntl.LOCK_UN)
        return blob
      else:
        remote_loc = os.path.join(self.remote_store.data_store, hash_key)
        f = open('/tmp/'+hash_key, 'w')
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if self.remoteCopy(remote_loc):
          fcntl.lockf(f, fcntl.LOCK_UN)
          return self.local_store.load(hash_key, blob_type)

    raise Exception('No such hash in store: ', hash_key)

  def save(self, hash_key, blob):
    self.local_store.save(hash_key, blob)

  def saveToFile(self, hash_key, blob, blob_type, ttl=28):
    self.local_store.saveToFile(hash_key, blob, blob_type, ttl)

  def delete(self, hash_key):
    self.local_store.delete(hash_key)
