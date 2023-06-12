import logging
import sys

try:
  import google.cloud.logging as cloud_logging
  from google.cloud.logging_v2.handlers import CloudLoggingHandler
  from google.cloud.logging_v2.handlers.transports import SyncTransport
except ImportError:
  pass

def getLogger(name, stderr=False, enable_cloud_logging=True):
  """
  Returns a logger object with the specified name. If stderr is True, logs will be sent to standard error instead of standard output.
  If enable_cloud_logging is True, logs will also be sent to Google Cloud Logging.
  
  Args:
  - name (str): Name of the logger.
  - stderr (bool): If True, logs will be written to stderr. Otherwise, logs will be written to stdout.
  - enable_cloud_logging (bool): If True, enables logging to Google Cloud.

  Returns:
  - logger (logging.Logger): A logger object with the specified name.
  """
  logger = logging.getLogger(name)
  logger.propagate = False
  logging.basicConfig()
  if not stderr:
    c_handler = logging.StreamHandler(stream=sys.stdout)
  else:
    c_handler = logging.StreamHandler(stream=sys.stderr)
  c_handler.setLevel(logging.DEBUG)
  formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
  c_handler.setFormatter(formatter)
  logger.addHandler(c_handler)
  if enable_cloud_logging:
    client = cloud_logging.Client()
    log_labels = {
      'component': name,
      'molecule': 'true'
    }
    cl_handler = CloudLoggingHandler(client, name=name, labels=log_labels, transport=SyncTransport)
    cl_handler.setLevel(logging.DEBUG)
    logger.addHandler(cl_handler)
  logger.setLevel(logging.DEBUG)
  return logger
