# Importing required libraries
import os
from urllib.parse import urlencode
import pytz
import logging
import subprocess
from datetime import datetime
from flask import Flask, Response, request, jsonify, make_response, render_template, redirect, url_for, send_from_directory
from flask_cors import CORS
from flask_pymongo import PyMongo
from crontab import CronTab, CronSlices
import gzip
from io import BytesIO
from molecule.zmqserver import ZMQServer, MessageBase

class GzipCompress:
  """
  A class to compress Flask response data using gzip.

  Attributes:
  - app (Flask): The Flask application instance.
  - compress_level (int): The level of compression to use (0-9).
  - minimum_size (int): The minimum size of response data to compress.
  """
  def __init__(self, app, compress_level=9, minimum_size=10):
    self.app = app
    self.compress_level = compress_level
    self.minimum_size = minimum_size
    self.app.after_request(self.after_request)

  def after_request(self, response):
    """
    Compresses Flask response data using gzip if the response meets certain criteria.

    Args:
    - response (Flask.Response): The Flask response object.

    Returns:
    - Flask.Response: The compressed Flask response object.
    """
    accept_encoding = request.headers.get('Accept-Encoding', '')

    # Check if response should be compressed
    if response.status_code < 200 or \
      response.status_code >= 300 or \
      response.direct_passthrough or \
      len(response.get_data()) < self.minimum_size or \
      'gzip' not in accept_encoding.lower() or \
      'Content-Encoding' in response.headers:
      return response

    # Compress response data using gzip
    gzip_buffer = BytesIO()
    gzip_file = gzip.GzipFile(
      mode='wb', compresslevel=self.compress_level, fileobj=gzip_buffer)
    gzip_file.write(response.get_data())
    gzip_file.close()
    response.set_data(gzip_buffer.getvalue())
    response.headers['Content-Encoding'] = 'gzip'
    response.headers['Content-Length'] = len(response.get_data())

    return response


# init app
app = Flask(__name__)
CORS(app)
GzipCompress(app)

mongo = PyMongo()

log = logging.getLogger('werkzeug')
log.setLevel(logging.CRITICAL)

UTC_TZ = pytz.timezone('UTC')
LOCAL_TZ = pytz.timezone('Asia/Kolkata')


@app.errorhandler(404) 
def not_found(e): 
  return render_template("error404.html") 


@app.route('/', methods=['GET'])
def index():
  return render_template('index.html', gcp_project=os.environ['GOOGLE_CLOUD_PROJECT'])


@app.route('/pipelines/', methods=['GET'])
def pipelines():
  return render_template('pipelines.html', gcp_project=os.environ['GOOGLE_CLOUD_PROJECT'])


@app.route('/pipelines/<p_hash>', methods=['GET'])
def view_pipeline(p_hash):
  res = mongo.db['pipelines'].find_one({'_id': p_hash})
  if res is None:
    return render_template('error404.html')
  res['timestamp'] = res['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  project_list = list(mongo.db['projects'].find({}, {'name': 1}))
  return render_template('view_pipeline.html', data=res, projects=project_list, gcp_project=os.environ['GOOGLE_CLOUD_PROJECT'])


@app.route('/pipelines/user/<username>', methods=['GET'])
def view_user_pipelines(username):
  return render_template('pipelines.html', username=username, gcp_project=os.environ['GOOGLE_CLOUD_PROJECT'])


@app.route('/projects/', methods=['GET'])
def projects():
  return render_template('projects.html', gcp_project=os.environ['GOOGLE_CLOUD_PROJECT'])


@app.route('/projects/add', methods=['GET'])
def add_project():
  res = {
    '_id': '',
    'name': '',
    'user': '',
    'timestamp': '',
    'status': 0,
    'scheduled': 0,
    'runs': dict(),
    'notes': '',
    'working_dir': '',
    'pipeline_name': '',
    'pipeline_spec_loc': '',
    'pipeline_config': '',
    'update_config': 0,
    'command': '',
    'schedule_str': ''
  }
  return render_template('view_project.html', data=res, pipelines=dict(), gcp_project=os.environ['GOOGLE_CLOUD_PROJECT'])


@app.route('/projects/<p_id>', methods=['GET'])
def view_project(p_id):
  res = mongo.db['projects'].find_one({'_id': p_id})
  res['timestamp'] = res['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  if res is None:
    return render_template('error404.html')
  pipeline_dict = dict()
  if res.get('runs', None) is not None:
    for p_hash, timestamp in res['runs'].items():
      pipeline_dict[timestamp.timestamp()] = mongo.db['pipelines'].find_one({'_id': p_hash}, {
        'name': 1, 'message': 1, 'status': 1, 'hash': 1, 'user': 1, 'project': 1, 'timestamp': 1, 'pinned': 1
      })
      pipeline_dict[timestamp.timestamp()]['timestamp'] = pipeline_dict[timestamp.timestamp()][
        'timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return render_template('view_project.html', data=res, pipelines=pipeline_dict, gcp_project=os.environ['GOOGLE_CLOUD_PROJECT'])


@app.route('/scheduler/', methods=['GET'])
def scheduler():
  res = mongo.db['projects'].find({}, {'name': 1, 'scheduled': 1, 'schedule_str': 1, 'command': 1})
  project_data = dict()
  for project in res:
    if project['name'] != 'default':
      project_data[project['_id']] = project
  return render_template('scheduler.html', projects=project_data, gcp_project=os.environ['GOOGLE_CLOUD_PROJECT'])


@app.route('/api/pipelines', methods=['GET'])
def get_pipelines():
  limit = request.args.get('limit', 500, type=int)
  data = mongo.db['pipelines'].find().limit(limit)
  res = dict()
  for pl in data:
    res[pl['_id']] = pl
    res[pl['_id']]['timestamp'] = pl['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))


@app.route('/api/pipelines/pinned', methods=['GET'])
def get_pinned_pipelines():
  data = mongo.db['pipelines'].find({'pinned': 1}, {
    'name': 1, 'message': 1, 'status': 1, 'hash': 1, 'user': 1, 'project': 1, 'timestamp': 1, 'pinned': 1
  }).sort('timestamp', -1)
  res = dict()
  for pl in data:
    res[pl['timestamp'].timestamp()] = pl
    res[pl['timestamp'].timestamp()]['timestamp'] = pl['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))


@app.route('/api/pipelines/unpinned', methods=['GET'])
def get_unpinned_pipelines():
  data = mongo.db['pipelines'].find({'pinned': 0}, {
    'name': 1, 'message': 1, 'status': 1, 'hash': 1, 'user': 1, 'project': 1, 'timestamp': 1, 'pinned': 1
  }).sort('timestamp', -1).limit(100)
  res = dict()
  for pl in data:
    res[pl['timestamp'].timestamp()] = pl
    res[pl['timestamp'].timestamp()]['timestamp'] = pl['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))


@app.route('/api/pipelines/project/<p_id>', methods=['GET'])
def get_project_pipelines(p_id):
  data = mongo.db['projects'].find_one({'_id': p_id})
  res = dict()
  for add_timestamp, p_hash in data['runs'].items():
    res[add_timestamp.timestamp()] = mongo.db['pipelines'].find_one({'_id': p_hash})
  return make_response(jsonify(res))


@app.route('/api/pipelines/user/<username>/pinned', methods=['GET'])
def get_user_pinned_pipelines(username):
  limit = request.args.get('limit', 500, type=int)
  data = mongo.db['pipelines'].find({'user': username, 'pinned': 1},
                                 {'name': 1, 'message': 1, 'status': 1, 'hash': 1,
                                  'user': 1, 'project': 1, 'timestamp': 1, 'pinned': 1}
                                 ).sort('timestamp', -1).limit(limit)
  res = dict()
  for pl in data:
    res[pl['timestamp'].timestamp()] = pl
    res[pl['timestamp'].timestamp()]['timestamp'] = pl['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))


@app.route('/api/pipelines/user/<username>/unpinned', methods=['GET'])
def get_user_unpinned_pipelines(username):
  limit = request.args.get('limit', 500, type=int)
  data = mongo.db['pipelines'].find({'user': username, 'pinned': 0},
                                 {'name': 1, 'message': 1, 'status': 1, 'hash': 1,
                                  'user': 1, 'project': 1, 'timestamp': 1, 'pinned': 1}
                                 ).sort('timestamp', -1).limit(limit)
  res = dict()
  for pl in data:
    res[pl['timestamp'].timestamp()] = pl
    res[pl['timestamp'].timestamp()]['timestamp'] = pl['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))


@app.route('/api/pipelines/recent', methods=['GET'])
def get_recent_pipelines():
  data = mongo.db['pipelines'].find({}, {'name': 1, 'message': 1, 'status': 1, 'hash': 1,
                                      'user': 1, 'project': 1, 'timestamp': 1, 'pinned': 1}
                                 ).sort('timestamp', -1).limit(5)
  res = dict()
  for pl in data:
    res[pl['timestamp'].timestamp()] = pl
    res[pl['timestamp'].timestamp()]['timestamp'] = pl['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))


@app.route('/api/pipelines/<p_hash>', methods=['GET'])
def get_pipeline(p_hash):
  res = mongo.db['pipelines'].find_one({'_id': p_hash})
  res['timestamp'] = res['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))


@app.route('/api/pipelines/<p_hash>/toggle_pin', methods=['GET'])
def toggle_pin_pipeline(p_hash):
  res = mongo.db['pipelines'].update_one({'_id': p_hash}, {'$bit': {'pinned': {'xor': 1}}})
  return make_response(jsonify(res))

@app.route('/api/pipelines/<p_hash>/update', methods=['POST'])
def update_pipeline_message(p_hash):
  current_project_name = mongo.db['pipelines'].find_one({'_id': p_hash})['project']
  mongo.db['projects'].update_one({'name': current_project_name}, {'$unset': {'runs.'+p_hash: 1}})
  mongo.db['projects'].update_one({'name': request.form['project']}, {'$set': {'runs.'+p_hash: datetime.now().astimezone()}})
  mongo.db['pipelines'].update_one({'_id': p_hash}, {'$set': {'message': request.form['message'], 'project': request.form['project']}})
  return make_response(jsonify({'current_project': current_project_name, 'new_project': request.form['project'], 'pipeline': p_hash}))


@app.route('/api/pipelines/submit', methods=['POST'])
def submit_pipeline():
  p_hash = request.json['hash']
  request.json['_id'] = p_hash
  request.json['pinned'] = 0
  request.json['status'] = 0
  request.json['timestamp'] = datetime.now().astimezone()
  res = mongo.db['pipelines'].update_one(
    {'_id': p_hash},
    request.json,
    upsert=True
  )
  return make_response(jsonify(res))


@app.route('/api/tasks', methods=['GET'])
def get_tasks():
  limit = request.args.get('limit', 100, type=int)
  data = mongo.db['tasks'].find().limit(limit)
  res = dict()
  for t in data:
    res[t['_id']] = t
    res[t['_id']]['timestamp'] = t['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
    res[t['_id']]['expires_at'] = t['expires_at'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
    if 'first_run_end_time' in t.keys():
      res[t['_id']]['first_run_end_time'] = t['first_run_end_time'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))

@app.route('/api/tasks/<t_hash>', methods=['GET'])
def get_task(t_hash):
  res = mongo.db['tasks'].find_one({'_id': t_hash})
  if res is not None:
    res['timestamp'] = res['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
    res['expires_at'] = res['expires_at'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
    if 'first_run_end_time' in res.keys():
      res['first_run_end_time'] = res['first_run_end_time'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
    return make_response(jsonify(res))
  else:
    return make_response(jsonify({}))


@app.route('/api/tasks/<t_hash>/status', methods=['GET'])
def get_task_status(t_hash):
  res = mongo.db['tasks'].find_one({'_id': t_hash})
  if res is not None:
    return make_response(jsonify({'status': res['status']}))
  else:
    return make_response(jsonify({'status': 0}))


@app.route('/api/tasks/submit', methods=['POST'])
def submit_task():
  t_hash = request.json['hash']
  request.json['_id'] = t_hash
  request.json['status'] = 0
  request.json['timestamp'] = datetime.now().astimezone(LOCAL_TZ)
  res = mongo.db['tasks'].update_one({'_id': t_hash}, request.json, upsert=True)
  return make_response(jsonify(res))


@app.route('/api/logs/molecule/<component>', methods=['GET'])
def get_component_logs(component):
  base_url = 'https://console.cloud.google.com/logs/query;query='
  query_params = {
    'labels.molecule': 'true',
    'labels.component': str(component)
  }
  url = base_url + urlencode(query_params) + ';timeRange=PT3H?project=' + os.environ['GOOGLE_CLOUD_PROJECT']
  url = url.replace('&', '%0A')
  return redirect(url)


@app.route('/api/logs/instance/<instance_id>', methods=['GET'])
def get_instance_logs(instance_id):
  base_url = 'https://console.cloud.google.com/logs/query;query='
  query_params = {
    'resource.labels.instance_id': str(instance_id),
    'logName': 'projects/' + os.environ['GOOGLE_CLOUD_PROJECT'] + '/logs/spawnerLog'
  }
  url = base_url + urlencode(query_params) + ';timeRange=PT3H?project=' + os.environ['GOOGLE_CLOUD_PROJECT']
  url = url.replace('&', '%0A')
  return redirect(url)


@app.route('/api/logs/transform/<t_hash>', methods=['GET'])
def get_transform_log(t_hash):
  base_url = 'https://console.cloud.google.com/logs/query;query='
  query_params = {
    'logName': 'projects/' + os.environ['GOOGLE_CLOUD_PROJECT'] + '/logs/' + str(t_hash),
    'labels.deployment_type': app.db_env
  }
  res = mongo.db['tasks'].find_one({'_id': t_hash})
  if res is not None:
    user_related_dir_string = res['data_dir'].split('users/')[1]
    user_name = user_related_dir_string.split('/')[0]
    query_params['labels.user'] = user_name
  url = base_url + urlencode(query_params) + '?project=' + os.environ['GOOGLE_CLOUD_PROJECT']
  url = url.replace('&', '%0A')
  return redirect(url)


@app.route('/api/platform/spawners', methods=['GET'])
def get_spawner_info():
  busy_spawners = list(mongo.db['workers'].find({'status': 3}))
  free_spawners = list(mongo.db['workers'].find({'status': 2}))
  res = {
    'busy_count': len(busy_spawners),
    'free_count': len(free_spawners),
    'busy_spawners': busy_spawners,
    'free_spawners': free_spawners
  }
  return make_response(jsonify(res))


@app.route('/api/platform/pipelines', methods=['GET'])
def get_pipelines_info():
  res = {
    'queued': mongo.db['pipelines'].count_documents({'status': 0}),
    'processing': mongo.db['pipelines'].count_documents({'status': 1}),
    'completed': mongo.db['pipelines'].count_documents({'status': 2}),
    'terminated': mongo.db['pipelines'].count_documents({'status': 3})
  }
  return make_response(jsonify(res))


@app.route('/api/platform/tasks', methods=['GET'])
def get_tasks_info():
  # res = {
  #   'queued': len(db.collection(app.db_env + '_' + 'tasks').where('status', '==', 0).get()),
  #   'processing': len(db.collection(app.db_env + '_' + 'tasks').where('status', '==', 1).get()),
  #   'completed': len(db.collection(app.db_env + '_' + 'tasks').where('status', '==', 2).get()),
  #   'terminated': len(db.collection(app.db_env + '_' + 'tasks').where('status', '==', 3).get())
  # }
  res = {
    'queued': mongo.db['tasks'].count_documents({'status': 0}),
    'processing': mongo.db['tasks'].count_documents({'status': 1}),
    'completed': mongo.db['tasks'].count_documents({'status': 2}),
    'terminated': mongo.db['tasks'].count_documents({'status': 3})
  }
  return make_response(jsonify(res))


@app.route('/api/projects/', methods=['GET'])
def get_projects():
  data = mongo.db['projects'].find({'name': {'$ne': 'default'}})
  res = dict()
  for pr in data:
    res[pr['timestamp'].timestamp()] = pr
    res[pr['timestamp'].timestamp()]['timestamp'] = pr['timestamp'].replace(tzinfo=pytz.utc).astimezone(LOCAL_TZ).strftime('%a, %d %b %Y %H:%M:%S IST')
  return make_response(jsonify(res))


@app.route('/api/projects/<p_id>/delete', methods=['GET'])
def delete_project(p_id):
  project = mongo.db['projects'].find_one({'_id': p_id})
  for p_hash in project.get('runs', dict()).keys():
    mongo.db['projects'].update_one({'name': 'default'}, {'$set': {'runs.'+str(p_hash): datetime.now().astimezone()}})
    mongo.db['pipelines'].update_one({'_id': p_hash}, {'$set': {'project': 'default'}})
  mongo.db['projects'].delete_one({'_id': p_id})
  return make_response(jsonify({'status': 'deleted'}))


def generateRunCommand(user, wd, pn, psl, pc, update_config, project):
  if int(update_config) == 1:
    uc = '-uc'
  else:
    uc = ''
  cmd_str = 'su {user} -c "cd {wd} && python3 core/infrautils.py ' + \
            '-c core/server_config.yaml -sp -pn {pn} -ps {ps} -pc {pc} {uc} -p {project}"'
  cmd_str = cmd_str.format(
    user=user,
    wd=wd,
    pn=pn,
    ps=psl,
    pc=pc,
    uc=uc,
    project=project
  )
  return cmd_str


@app.route('/api/projects/update', methods=['POST'])
def update_project():
  form_data = request.form.to_dict()
  if form_data.get('timestamp', None) == '':
    form_data['timestamp'] = datetime.now()
  else:
    del form_data['timestamp']
  form_data['command'] = generateRunCommand(form_data['user'], form_data['working_dir'],
                                            form_data['pipeline_name'], form_data['pipeline_spec_loc'],
                                            form_data['pipeline_config'], form_data['update_config'], 
                                            form_data['name'])
  form_data['status'] = int(form_data['status'])
  form_data['update_config'] = int(form_data['update_config'])
  form_data['scheduled'] = int(form_data['scheduled'])
  mongo.db['projects'].update_one({'_id': form_data['_id']}, {'$set': form_data}, upsert=True)
  return make_response(jsonify({'status': 'updated'}))


@app.route('/api/projects/<p_id>/run', methods=['GET'])
def run_project(p_id):
  project = mongo.db['projects'].find_one({'_id': p_id}, {'command': 1})
  test_run = subprocess.run(project['command'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  if test_run.returncode == 0:
    status = 'Run successful'
  else:
    status = 'Run failed'
  return make_response(jsonify({'status': status, 
                                'stdout': test_run.stdout.decode('utf-8'), 
                                'stderr': test_run.stderr.decode('utf-8')}))


@app.route('/api/projects/<p_id>/schedule', methods=['POST'])
def schedule_project(p_id):
  cron = CronTab(user='root')
  schedule_str = request.form['schedule_str']
  mongo.db['projects'].update_one(
    {'_id': p_id},
    {'$set': {'schedule_str': schedule_str, 'scheduled': 1}},
    upsert=True
  )
  project = mongo.db['projects'].find_one({'_id': p_id}, {'command': 1})
  for job in cron.find_comment(p_id):
    cron.remove(job)
  job = cron.new(command=project['command'], comment=project['_id'])
  if CronSlices.is_valid(schedule_str):
    job.slices = CronSlices(schedule_str)
    cron.write()
  else:
    return make_response(jsonify({'status': 'Invalid Schedule'}))
  return make_response(jsonify({'status': 'Scheduled'}))


@app.route('/api/projects/<p_id>/unschedule', methods=['GET'])
def unschedule_project(p_id):
  cron = CronTab(user='root')
  for job in cron.find_comment(p_id):
    cron.remove(job)
    cron.write()
  mongo.db['projects'].update_one(
    {'_id': p_id},
    {'$set': {'schedule_str': '', 'scheduled': 0}},
    upsert=True
  )
  return make_response(jsonify({'status': 'Unscheduled'}))


@app.route('/api/notifications/', methods=['GET'])
def get_notifications():
  data = mongo.db['notifications'].find({'active': 1}).sort('timestamp', -1)
  res = dict()
  for notify in data:
    res[notify['timestamp'].timestamp()] = notify
  return make_response(jsonify(res))


@app.route('/api/notifications/<n_id>/dismiss', methods=['GET'])
def dismiss_notification(n_id):
  mongo.db['notifications'].update_one(
    {'_id': n_id},
    {'$set': {'active': 0}},
    upsert=True
  )
  return make_response(jsonify({"message": "dismissed"}))


@app.route('/api/download/details', methods=['POST'])
def get_file_list():
  is_prod = True if request.form['has_git'] == 'true' else False
  user = request.form['user']
  dir_hash = request.form['o_hash']
  if is_prod:
    search_dir = os.path.join('/mnt/data/store/data', dir_hash)
  else:
    search_dir = os.path.join('/mnt/data', user, 'store/data', dir_hash)
  if not os.path.exists(search_dir):
    return make_response(jsonify({'status': 0}))
  file_list = os.listdir(search_dir)
  return make_response(jsonify({'status': 1, 'has_git': is_prod, 'user': user, 'o_name': request.form['o_name'],
                                'o_hash': dir_hash, 'file_list': file_list}))


@app.route('/api/download/start', methods=['GET', 'POST'])
def get_file():
  is_prod = True if request.args.get('has_git') == 'true' else False
  user = request.args.get('user')
  dir_hash = request.args.get('o_hash')
  filename = request.args.get('filename')
  if is_prod:
    dir_loc = os.path.join('/mnt/data/molecule/store/data', dir_hash)
  else:
    dir_loc = os.path.join('/mnt/data/', user, 'store/data', dir_hash)
  file_loc = os.path.join(dir_loc, filename)
  if not os.path.exists(file_loc):
    return make_response('file not found', 404)
  file_ext = filename.split('.')[-1]
  if file_ext in ('tsv', 'csv', 'yaml'):
    with open(file_loc, 'r') as f:
      file = f.read()
    return Response(
      file,
      mimetype="text/"+file_ext,
      headers={"Content-disposition":
                "attachment; filename="+filename})
  return send_from_directory(dir_loc, filename, as_attachment=True)


@app.route('/api/command', methods=['POST'])
def command():
  # accept command string and send it to the server
  command = request.json
  cmd_str = command['cmd_str']
  cmd_param = command['cmd_param']
  expected_response = command['expected_response']
  # version = command['version']
  if command.get('version', '') != MessageBase.VERSION:
    return make_response(jsonify({'response': 'Version mismatch'}))
  out_sock = ZMQServer.getClientSock('localhost', app.server_port)
  resp = ZMQServer.sendCommand(sock=out_sock, cmd_str=cmd_str,
                               cmd_param=cmd_param, expected_response=expected_response)
  ZMQServer.closeSocket(out_sock)
  return make_response(jsonify({"response": resp}))


def startWebServer(port, server_port, session_port, scheduler_port, db_env):
  """
  Starts the web server with the given configuration.

  Args:
  - port (int): The port number to run the server on.
  - server_port (int): The port number to use for the server.
  - session_port (int): The port number to use for the session.
  - scheduler_port (int): The port number to use for the scheduler.
  - db_env (str): The name of the database environment to use.

  Returns:
  - None
  """
  global app
  app.__dict__.update({
    'server_port': server_port,
    'session_port': session_port,
    'scheduler_port': scheduler_port,
    'db_env': db_env
  })
  if os.environ.get('MONGO_URI'):
    app.config["MONGO_URI"] = str(os.environ.get('MONGO_URI')) + '/' + str(db_env)
  else:
    app.config["MONGO_URI"] = "mongodb://mongo0:27017,mongo1:27017/" + str(db_env)
  global mongo
  mongo = PyMongo(app)
  app.run(debug=False, host='0.0.0.0', port=port, threaded=True)
