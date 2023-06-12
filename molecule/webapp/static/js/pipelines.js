function getPipelinesData() {
  getPinnedPipelines();
  getUnpinnedPipelines();
}

function getPinnedPipelines() {
  let uri = url + 'api/pipelines/pinned';
  if (username !== '')
    uri = url + 'api/pipelines/user/' + username + '/pinned';
  getPipelines(uri, '#pinned-pipelines', getPipelinesData);
}

function getUnpinnedPipelines() {
  let uri = url + 'api/pipelines/unpinned';
  if (username !== '')
    uri = url + 'api/pipelines/user/' + username + '/unpinned';
  getPipelines(uri, '#unpinned-pipelines', getPipelinesData);
}

window.onload = function () {
  updatePage();
}

function updatePage() {
  getPipelinesData();
}

var intervalID = window.setInterval(function () {
  getPipelinesData();
}, 5000)

document.addEventListener("visibilitychange", function() {
	if (!document.hidden) {
    getPipelinesData();
    window.clearInterval(intervalID);
    intervalID = window.setInterval(function () {
      getPipelinesData();
    }, 5000)
  } else {
    window.clearInterval(intervalID);
  }
});
