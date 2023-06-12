function getDashboardData() {
  updatePipelines();
  updateTasks();
  updateSpawners();
  getRecentPipelines();
}

function updateSpawners() {
  let uri = url + 'api/platform/spawners';
  fetch(uri)
    .then(response => response.json())
    .then(data => {
      $('#busy-spawners').text(data['busy_count']);
      $('#total-spawners').text(data['busy_count'] + data['free_count']);
    })
}

function updatePipelines() {
  let uri = url + 'api/platform/pipelines';
  fetch(uri)
    .then(response => response.json())
    .then(data => {
      $('#pipelines-processing').text(data['queued'] + data['processing']);
      $('#pipelines-completed').text(data['completed'] + data['terminated']);
    })
}

function updateTasks() {
  let uri = url + 'api/platform/tasks';
  fetch(uri)
    .then(response => response.json())
    .then(data => {
      $('#tasks-remaining').text(data['queued']+data['processing'])
    })
}

function getRecentPipelines() {
  let uri = url + 'api/pipelines/recent';
  getPipelines(uri, '#recent-pipelines', getDashboardData);
}

window.onload = function () {
  updatePage();
}

function updatePage() {
  getDashboardData();
}

var intervalID = window.setInterval(function () {
  getDashboardData();
}, 3000)

document.addEventListener("visibilitychange", function() {
	if (!document.hidden) {
    getDashboardData();
    window.clearInterval(intervalID);
    intervalID = window.setInterval(function () {
      getDashboardData();
    }, 3000)
  } else {
    window.clearInterval(intervalID);
  }
});
