const output = document.getElementById('log-data');
window.worker_type = 'all'
window.worker = 'platform_server';

$('#worker-types').change(function () {
  window.worker_type = $(this).val();
  updateWorkerList(window.worker_type);
}).mousedown(function () {
  $(this).val('');
})

$('#workers').change(function() {
  window.worker = $(this).val();
  getLog(window.worker);
}).mousedown(function () {
  $(this).val('');
})

function getLog(worker) {
  let uri = url + '/api/logs/'+worker;
  fetch(uri).then(response => response.text())
      .then(data => {
        output.textContent = data;
      })
}

function updateWorkerList(worker_type) {
  let html_snippet = '';
  if (workers.hasOwnProperty(worker_type)) {
    for (let worker of workers[worker_type]) {
      html_snippet += '<option value="' + worker + '"></option>';
    }
  }
  $('#worker-list').html(html_snippet);
  getLog(workers[worker_type][0]);
  $('#workers').val(workers[worker_type][0]);
}

setInterval(function() {
  getLog(window.worker)
}, 5000);

window.onload = function () {
  updatePage();
}

function updatePage() {
  getLog('platform_server');
  let html_snippet = '';
  for (let worker_type of Object.keys(workers)) {
    html_snippet += '<option value="' + worker_type + '"></option>';
  }
  $('#worker-type-list').html(html_snippet);
  updateWorkerList('all');
}