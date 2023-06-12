function getProjectsData() {
  let uri = url + '/api/projects'
  getProjects(uri,'#projects-list');
}

window.onload = function () {
  updatePage();
}

function updatePage() {
  getProjectsData();
}

var intervalID = window.setInterval(function () {
  getProjectsData();
}, 5000)

document.addEventListener("visibilitychange", function() {
	if (!document.hidden) {
    getProjectsData();
    window.clearInterval(intervalID);
    intervalID = window.setInterval(function () {
      getProjectsData();
    }, 5000)
  } else {
    window.clearInterval(intervalID);
  }
});
