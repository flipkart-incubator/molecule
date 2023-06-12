from unittest import TestCase
import yaml
import os
import threading
import subprocess
import time

from threading import Thread
from queue import Queue, Empty

os.chdir(os.path.join(os.getcwd(), os.path.dirname(__file__), '../../..'))
print(os.getcwd())

class NonBlockingStreamReader:
  def __init__(self, stream):
    self._s = stream
    self._q = Queue()

    def _populateQueue(streamed, queue):
      while True:
        try:
          line = streamed.readline()
        except Exception:
          break
        if line:
          queue.put(line)
        else:
          pass

    self._t = Thread(target=_populateQueue, args=(self._s, self._q))
    self._t.daemon = True
    self._t.start()

  def readline(self, timeout=None):
    try:
      return self._q.get(block=timeout is not None, timeout=timeout)
    except Empty:
      return None

class Runner:
  def __init__(self):
    self.platform = None
    self.spawner = None
    self.submit = None
    self.t_hashes = [
      '53084fa97aa699e571851ab614bf26e4',
      'c3b9460e6de95645f508c89d431e8714',
      '47f8cd3d0b9d0ca4f2457d8e99012791'
    ]
    self.processes = dict()

  def firePlatform(self, server_cfg):
    cmd = 'python3 core/infrautils.py -c ' + server_cfg + ' -ss'
    print(cmd)
    cmd = cmd.split(' ')
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    self.processes.update(platform=p)
    stdout_lines = []
    nbsr = NonBlockingStreamReader(p.stdout)
    while True:
      while True:
        output = nbsr.readline(0.1)
        if not output:
          # print('no more data')
          break
        stdout_lines.append(output.decode('utf-8'))
      if 'received response:  no reply' in stdout_lines:
        self.platform = False
        p.terminate()
        break
      mark = True
      for t_hash in self.t_hashes:
        if t_hash not in ' '.join(stdout_lines):
          mark = False
      if mark:
        self.platform = True
        break

  def fireSpawner(self):
    cmd = 'python3 core/spawner.py -cs localhost:11115 -p 11110,11111,11112 -s ' \
          + os.path.join('/tmp', 'store') + ' -wd ' + os.getcwd()
    print(cmd)
    cmd = cmd.split(' ')
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    self.processes.update(spawner=p)
    stdout_lines = []
    nbsr = NonBlockingStreamReader(p.stdout)
    while True:
      while True:
        output = nbsr.readline(0.1)
        if not output:
          # print('no more data')
          break
        stdout_lines.append(output.decode('utf-8'))
      if 'received response:  ack_fail_transform' in stdout_lines:
        self.spawner = False
        p.terminate()
        break
      count = 0
      for line in stdout_lines:
        if 'received response:  ack_finish_transform' in line:
          count += 1
      if count == 3:
        self.spawner = True
        p.terminate()
        self.processes['platform'].terminate()
        break

  def submitPipeline(self, loc, server_cfg, pipeline_name, pipeline_config):
    cmd = 'python3 core/infrautils.py -c ' + server_cfg + ' -d -sp -pn ' \
          + pipeline_name + ' -ps ' + loc + ' -pc ' + pipeline_config
    print(cmd)
    cmd = cmd.split(' ')
    p = subprocess.run(cmd, stdout=subprocess.PIPE)
    if p.returncode == 0:
      self.submit = True
    else:
      self.submit = False


class TestDummyPipeline(TestCase):
  def setUp(self):
    self.loc = 'projects/dummy'

    self.store = os.path.join('/tmp', 'store')
    subprocess.run('rm -rf /tmp/store', shell=True)
    if not os.path.exists(self.store):
      os.makedirs(self.store)

    self.server_cfg = {
      'root_dir': os.getcwd(),
      'store_loc': self.store,
      'server_ip': 'localhost',
      'server_port': 11115,
      'session_ip': 'localhost',
      'session_port': 11116,
      'scheduler_ip': 'localhost',
      'scheduler_port': 11117,
      'worker_list': {
        'cpu': [
          'localhost:11110',
          'localhost:11111',
          'localhost:11112'
        ]
      }
    }

    self.server_cfg_file = os.path.join('/tmp', 'server_cfg.yaml')
    with open(self.server_cfg_file, 'w') as file:
      yaml.dump(self.server_cfg, file)

  def test_dummy_pipeline(self):
    pipeline_name = 'dummy_pipeline'
    config = os.path.join(self.loc, 'configs', pipeline_name + '_config.yaml')

    runner = Runner()
    spawner = threading.Thread(target=runner.fireSpawner, daemon=True)
    platform = threading.Thread(target=runner.firePlatform, args=(self.server_cfg_file,), daemon=True)
    submit = threading.Thread(target=runner.submitPipeline,
                              args=(self.loc, self.server_cfg_file, pipeline_name, config))

    spawner.start()
    time.sleep(1)
    platform.start()
    time.sleep(1)
    submit.start()

    submit.join()
    if runner.submit is not None:
      self.assertTrue(runner.submit)
      if not runner.submit:
        for _, p in runner.processes.items():
          p.kill()
          p.wait()
          # stderr = [err.decode('utf-8') for err in p.stderr.readlines()]
          # print(stderr)
        platform.join()
        spawner.join()
        exit(0)

    while True:
      time.sleep(1)
      if runner.platform is not None:
        self.assertTrue(runner.platform)
      if runner.spawner is not None:
        self.assertTrue(runner.spawner)
      if runner.spawner is not None and runner.platform is not None:
        platform.join()
        spawner.join()
        for _, p in runner.processes.items():
          p.kill()
          p.wait()
          # stderr = [err.decode('utf-8') for err in p.stderr.readlines()]
          # print(''.join(stderr))
          p.stderr.close()
          p.stdout.close()
        break
