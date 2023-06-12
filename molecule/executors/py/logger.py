import logging
import time
import sys
try:
  import google.cloud.logging as cloud_logging
  from google.cloud.logging_v2.handlers import CloudLoggingHandler
  from google.cloud.logging_v2.handlers.transports import SyncTransport
except ImportError:
  pass

def getLogger(hash_key, user=None, deployment_type='prod', stderr=False, enable_cloud_logging=False):
  logger = logging.getLogger(hash_key)
  if not stderr:
    c_handler = logging.StreamHandler(stream=sys.stdout)
  else:
    c_handler = logging.StreamHandler(stream=sys.stderr)
  c_handler.setLevel(logging.DEBUG)
  c_formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
  c_handler.setFormatter(c_formatter)
  logger.addHandler(c_handler)
  if enable_cloud_logging:
    client = cloud_logging.Client()
    log_timestamp = str(int(time.time()))
    log_labels = {
      'transform_hash': hash_key,
      'log_timestamp': log_timestamp,
      'molecule': 'true',
      'deployment_type': deployment_type
    }
    if user is not None:
      log_labels['user'] = user
    cl_handler = CloudLoggingHandler(client, name=hash_key, labels=log_labels, transport=SyncTransport)
    cl_handler.setLevel(logging.DEBUG)
    logger.addHandler(cl_handler)
  logger.propagate = False
  logger.setLevel(logging.DEBUG)
  return logger

