import zmq, traceback
import yaml

from . import pl_logger as logger

log = logger.getLogger('zmqserver', enable_cloud_logging=False)

yaml.Dumper.ignore_aliases = lambda *args : True

class MessageBase:
  """
  A class that defines the message format for communication between the client and server.
  """
  VERSION = "20230327"
  
  COMMAND_KEY = "command"
  COMMAND_PARAM_KEY = "command_param"
  WORKER_ID = "worker_id"
  TRANSFORM_KEY = "transform"

  TRANSFORM_HASH = "transform_hash"
  TRANSFORM_PRECOMPUTED = "transform_precomputed"
  TRANSFORM_CLASSNAME = "transform_class_name"

  COMMAND_HEARTBEAT = "heartbeat"
  COMMAND_CONNECT = "connect_worker"
  COMMAND_RECONNECT = "reconnect_worker"
  COMMAND_KILL = "kill_worker"
  COMMAND_SUBMIT_GRAPH = "submit_graph"
  COMMAND_DELETE_GRAPH = "delete_graph"
  COMMAND_PROCESS_GRAPH = "process_graph"
  COMMAND_FINISH_TRANSFORM = "finish_transform"
  COMMAND_PROCESS_FINISH_TRANSFORM = "process_finish_transform"
  COMMAND_PROCESS_FAILED_TRANSFORM = "process_failed_transform"
  COMMAND_ADD_TRANSFORM = "add_unfinished_transform"
  COMMAND_GET_NEXT_TRANSFORM = "get_next_transform"
  COMMAND_GET_CHECK_TRANSFORM = "get_check_transforms"
  COMMAND_START_TRANSFORM = "start_transform"
  COMMAND_ALLOCATE_WORKER = "allocate_worker"
  COMMAND_FAIL_TRANSFORM = "fail_transform"
  COMMAND_REFRESH_AUTOSCALE_POLICY = "refresh_autoscale_policy"

  ACK_HEARTBEAT = "ack_heartbeat"
  ACK_CONNECT = "ack_connect_worker"
  ACK_RECONNECT = "ack_reconnect_worker"
  ACK_KILL = "ack_kill_worker"
  ACK_SUBMIT_GRAPH = "ack_submit_graph"
  ACK_DELETE_GRAPH = "ack_delete_graph"
  ACK_PROCESS_GRAPH = "ack_process_graph"
  ACK_FINISH_TRANSFORM = "ack_finish_transform"
  ACK_PROCESS_FINISH_TRANSFORM = "ack_process_finish_transform"
  ACK_GET_NEXT_TRANSFORM = "ack_get_next_transform"
  ACK_START_TRANSFORM = "ack_start_transform"
  ACK_ALLOCATE_WORKER = "ack_allocate_worker"
  ACK_FAIL_TRANSFORM = "ack_fail_transform"
  ACK_SCHEDULE_TRANSFORM = "ack_schedule_transform"
  ACK_REFRESH = "ack_refresh"
  ACK_DEFAULT = "ack"
  ACK_ERROR = "error"

  RESPONSE_GRAPH_PROCESSED = "graph_processed"
  RESPONSE_TRANSFORM_PROCESSED = "transform_processed"
  RESPONSE_NEXT_TRANSFORM = "next_transform"
  RESPONSE_CHECK_TRANSFORM = "check_transform"
  RESPONSE_ADD_TRANSFORM = "add_transform"
  
  REQUEST_NEW_FREE_WORKER = "request_new_free_worker"
  REQUEST_FREE_WORKER = "request_free_worker"
  WORKER_ASSIGNED = "worker_assigned"
  WORKER_CREATION_QUEUED = "worker_creation_queued"
  WORKER_POLICY_WAIT = "worker_policy_wait"
  WORKER_NOT_AVAILABLE = "worker_not_available"
  MARK_FREE_WORKER = "mark_free_worker"
  MARK_STALE_WORKER = "mark_stale_worker"


class ZMQServer:
  """
  A class that defines the ZMQ server and its methods for sending and receiving messages.
  """
  def __init__(self):
    pass

  @staticmethod
  def getClientSock(in_ip, in_port, relaxed=True):
    """
    A static method that returns a client socket for the given IP address and port number.
    """
    task_status_cli_context = zmq.Context()
    task_status_cli_socket = task_status_cli_context.socket(zmq.REQ)
    if relaxed:
      task_status_cli_socket.setsockopt(zmq.RCVTIMEO, 5000)
      task_status_cli_socket.setsockopt(zmq.REQ_RELAXED, 1)
    task_status_cli_socket.connect("tcp://" + in_ip + ":" + str(in_port))

    return task_status_cli_socket

  @staticmethod
  def getServerSock(in_port):
    """
    A static method that returns a server socket for the given port number.
    """
    task_status_srv_context = zmq.Context()
    task_status_srv_socket = task_status_srv_context.socket(zmq.REP)
    task_status_srv_socket.bind("tcp://*:" + str(in_port))

    return task_status_srv_socket

  @staticmethod
  def sendCommand(sock, cmd_str, cmd_param, expected_response, max_tries=3):
    """
    A static method that sends a command to the given socket and waits for the expected response.
    """
    cmd = {
      MessageBase.COMMAND_KEY: cmd_str,
      MessageBase.COMMAND_PARAM_KEY: cmd_param
    }
    response = ""
    n_tries = max_tries

    while (response != expected_response) and (n_tries > 0):
      try:
        yaml_str = yaml.dump(cmd)
      except:
        raise Exception('serializing YAML to str failed')
      sock.send_string(yaml_str)
      try:
        response = sock.recv_string()
      except Exception as _:
        response = "no reply"
      n_tries = n_tries - 1
    if n_tries == 0 and response == 'no reply':
      log.critical('Connection Failed')

    if response == expected_response:
      # log.info('Success!')
      pass
    else:
      log.critical('Failed!')
      print('received response: ' + response)
      print('expected response: ' + expected_response)

    return response

  @staticmethod
  def sendRequest(sock, cmd_str, cmd_param):
    """
    A static method that sends a request to the given socket and returns the response.
    """
    cmd = {MessageBase.COMMAND_KEY: cmd_str, MessageBase.COMMAND_PARAM_KEY: cmd_param}

    try:
      yaml_str = yaml.dump(cmd)
    except:
      raise Exception('serializing YAML to str failed')
    sock.send_string(yaml_str)

    recv_yaml_str = sock.recv_string()
    try:
      response = yaml.load(recv_yaml_str, Loader=yaml.Loader)
    except:
      raise Exception('parsing str to YAML failed')

    return response

  @staticmethod
  def sendResponse(sock, cmd_str, cmd_param):
    """
    A static method that sends a response to the given socket.
    """
    cmd = {MessageBase.COMMAND_KEY: cmd_str, MessageBase.COMMAND_PARAM_KEY: cmd_param}
    try:
      yaml_str = yaml.dump(cmd)
    except:
      raise Exception('serializing YAML to str failed')
    sock.send_string(yaml_str)

  @staticmethod
  def sendACK(sock, ack_message):
    """
    A static method that sends an acknowledgement message to the given socket.
    """
    sock.send_string(ack_message)

  @staticmethod
  def getMessage(sock):
    """
    A static method that receives a message from the given socket and returns it.
    """
    msg = sock.recv_string()
    try:
      response = yaml.load(msg, Loader=yaml.Loader)
    except Exception as e:
      print(e)
      print(traceback.format_exc())
      response = {
        MessageBase.COMMAND_KEY: MessageBase.ACK_ERROR,
        MessageBase.COMMAND_PARAM_KEY: 'parsing str to YAML failed'
      }
    return response

  @staticmethod
  def closeSocket(sock):
    """
    A static method that closes the given socket.
    """
    sock.close(linger=0)
    del sock
