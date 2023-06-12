# Importing required modules
import os
import yaml
import socket
import argparse
import multiprocessing
import subprocess

# Importing custom modules
from .resources import setEnvVars
from . import pl_logger as logger
from . import server, session, scheduler, autoscaler, database, pl_storage
from .webapp import app as flask_app

# Class to start the platform
class StartPlatform:
  def __init__(self, config, db_env):
    """
    Initializes the StartPlatform class.

    Args:
    config (dict): A dictionary containing the configuration variables for the platform.
    db_env (str): A string indicating the environment for the database.
    """
    self.config = config
    self.store = pl_storage.PipelineStorage(self.config['store_loc'])
    self.db_env = db_env
    self.db = None

  def startLocalMongo(self):
    """
    Starts a local MongoDB server.
    """
    # check if mongo is installed otherwise raise Exception
    if not subprocess.run('which mongod', shell=True).returncode == 0:
      raise Exception('MongoDB is not installed. Please install MongoDB and try again.')
    # check db_path exists otherwise create it
    db_path = os.path.join(self.config['store_loc'], 'mongo')
    if not os.path.exists(db_path):
      os.makedirs(db_path)
    # check if mongo is already running
    # ps -ax | grep mongo
    p = subprocess.run('ps -ax | grep mongod | grep -v grep', shell=True, stdout=subprocess.PIPE)
    if p.stdout.decode('utf-8').count('mongod') > 1:
      print('MongoDB is already running.')
      return
    subprocess.Popen(' '.join(['mongod --dbpath', db_path]), shell=True, stdout=subprocess.PIPE)

  def initDb(self):
    """
    Initializes the database.
    """
    self.db = database.Database(self.db_env)

  def startServer(self):
    """
    Starts the server.
    """
    self.server = server.Server(self.config['server_port'], self.config['scheduler_ip'], self.config['scheduler_port'],
                                self.config['session_ip'], self.config['session_port'],
                                self.config['autoscaler_ip'], self.config['autoscaler_port'],
                                self.store, self.db)

    self.server.startServer()

  def startWebServer(self, db_env='stage'):
    """
    Starts the web server.

    Args:
    db_env (str): A string indicating the environment for the database.
    """
    flask_app.startWebServer(self.config['webserver_port'], self.config['server_port'], 
                             self.config['session_port'], self.config['scheduler_port'], db_env)

  def startScheduler(self):
    """
    Starts the scheduler.
    """
    self.scheduler = scheduler.Scheduler(self.config['scheduler_port'], self.store, self.db)
    self.scheduler.startScheduler()

  def startSession(self):
    """
    Starts the session.
    """
    seq_session_p0 = session.SeqSession(self.db)
    seq_session_p1 = session.SeqSession(self.db)
    seq_session_p2 = session.SeqSession(self.db)
    seq_sessions = (seq_session_p0, seq_session_p1, seq_session_p2)
    self.session = session.Session(seq_sessions, self.config['session_port'], self.store)
    self.session.startSession()
    
  def startAutoscaler(self):
    """
    Starts the autoscaler.
    """
    self.asp = autoscaler.AutoScalingPolicy(self.db, self.db_env)
    self.autoscaler = autoscaler.Autoscaler(self.config['autoscaler_port'], self.config['server_ip'], self.config['server_port'], self.config['platforms'], self.asp, self.db)
    self.autoscaler.startAutoscaler()

# Main function
def main():
  multiprocessing.set_start_method('fork')
  arg_parser = argparse.ArgumentParser(description='utility for managing pipelines services')

  # Adding command line arguments
  arg_parser.add_argument('-c', '--server-config', action='store', help='Path to server configuration variables')
  arg_parser.add_argument('-ss', '--start-services', action='store_true', help='start services')
  arg_parser.add_argument('-sws', '--start-web-services', action='store_true', help='start web services')
  arg_parser.add_argument('-o', '--offline', action='store_true', help='offline mode')

  # Parsing command line arguments
  args = arg_parser.parse_args()
  
  # Initializing logger
  log = logger.getLogger('setup', enable_cloud_logging=False)

  # Loading configuration from file
  config = None
  # check if args.server_config is a file
  # if not then default to ~/.molecule/server_config.yaml
  # if that doesn't exist then raise Exception
  if args.server_config is None:
    args.server_config = os.path.expanduser('~/.molecule/server_config.yaml')
  if not os.path.isfile(args.server_config):
    raise Exception('server-config file not found')
  setEnvVars(args.server_config)
  with open(args.server_config, 'r') as fl:
    config = yaml.load(fl, Loader=yaml.Loader)

  # Setting server IP if not offline and services are being started
  if not args.offline and args.start_services:
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    config['server_ip'] = str(ip)

  # Setting deployment type label
  deployment_type_label = config['DEPLOYMENT_TYPE_LABEL']

  # Initializing StartPlatform class
  setup = StartPlatform(config, deployment_type_label)

  # Starting local mongo if specified
  if args.start_services and args.offline:
    os.environ['MONGO_URI'] = 'mongodb://localhost:27017'
    setup.startLocalMongo()
    log.info('local mongo start successful!')

  # Starting services if specified
  if args.start_services:
    setup.initDb()
    setup.db.setPlatformConfig(deployment_type_label, config['server_ip'], config['server_port'])
    log.info('connected to ' + deployment_type_label + ' db!')
    setup.startServer()
    log.info('server start successful!')
    setup.startScheduler()
    log.info('scheduler start successful!')
    setup.startSession()
    log.info('session start successful!')
    setup.startAutoscaler()
    log.info('autoscaler start successful')

  # Starting web services if specified
  if args.start_web_services:
    ui_process = multiprocessing.Process(target=setup.startWebServer, args=(deployment_type_label,))
    ui_process.start()
    log.info('webserver start successful!')
    setup.db.pushNotification(2, 'Molecule Started!', 'DS Infra boot up successful')
    ui_process.join()

if __name__ == '__main__':
  main()
