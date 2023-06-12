// base URL setting
const url = window.location.origin + '/';

function togglePipelinePin(p_hash, update_function) {
  let uri = url + 'api/pipelines/' + p_hash + '/toggle_pin';
  fetch(uri)
    .then(response => response.json())
    .then(() => {
      update_function();
    })
}

function getPipelines(uri, update_id, update_function) {
  fetch(uri)
    .then(response => response.json())
    .then(data => {
      populatePipelines(data, update_id, update_function);
    })
}

function populatePipelines(data, update_id, update_function=undefined) {
  let html_snippet = '';
  for (let p_timestamp of Object.keys(data).reverse()) {
    if (data.hasOwnProperty(p_timestamp)) {
      let pipeline = data[p_timestamp];
      let status = 'Terminated';
      let badge = 'badge-gradient-danger';
      let pin = 'mdi-pin-off';
      if (pipeline['status'] === 0) {
        status = 'Queued';
        badge = 'badge-gradient-info';
      } else if (pipeline['status'] === 1) {
        status = 'Processing';
        badge = 'badge-gradient-primary';
      } else if (pipeline['status'] === 2) {
        status = 'Completed';
        badge = 'badge-gradient-success';
      }
      if (pipeline['pinned'] === 1) {
        pin = 'mdi-pin';
      }
      html_snippet += `
          <tr>
            <td> ` + pipeline['name'] + ` </td>
            <td> ` + pipeline['message'].substring(0, 15) + ` </td>
            <td> <label class="badge ` + badge + `">` + status + `</label> </td>
            <td><a href="/pipelines/` + pipeline['hash'] + `"> ` + pipeline['hash'] + ` </a></td>
            <td><a href="/pipelines/user/` + pipeline['user'] + `"> ` + pipeline['user'] + ` </td>
            <td> ` + pipeline['project'] + ` </td>
            <td> ` + pipeline['timestamp'] + ` </td>
            <td>`
      if (update_function === undefined) {
        html_snippet += `<i class="mdi ` + pin + `"></i>`;
      } else {
        html_snippet += `
              <span onclick="togglePipelinePin('` + pipeline['hash'] + `', ` + update_function + `)">
                <i class="mdi ` + pin + `"></i>
              </span>`;
      }
      html_snippet += `
            </td>
          </tr>`;
    }
  }
  $(update_id).html(html_snippet);
}

function getProjects(uri, update_id) {
  fetch(uri)
    .then(response => response.json())
    .then(data => {
      let html_snippet = '';
      for (let p_timestamp of Object.keys(data).reverse()) {
        if (data.hasOwnProperty(p_timestamp)) {
          let project = data[p_timestamp];
          /**
           * Created - dark - 0
           * On Hold - primary - 1
           * Experimentation - info - 2
           * Shadow - warning - 3
           * Completed - success - 4
           * Onboarded - light - 5
           * Closed - danger -6
          **/
          let badge = 'badge-gradient-dark' ;
          let status = 'Created';
          switch (project['status']) {
            case 0:
              badge = 'badge-gradient-dark';
              status = 'Created';
              break;
            case 1:
              badge = 'badge-gradient-primary';
              status = 'On Hold';
              break;
            case 2:
              badge = 'badge-gradient-info';
              status = 'Experimentation';
              break;
            case 3:
              badge = 'badge-gradient-warning';
              status = 'Shadow';
              break;
            case 4:
              badge = 'badge-gradient-success';
              status = 'Completed';
              break;
            case 5:
              badge = 'badge-gradient-secondary';
              status = 'Onboarded';
              break;
            case 6:
              badge = 'badge-gradient-danger';
              status = 'Closed';
              break;
          }
          let tick = 'mdi-checkbox-blank-outline';
          if (project['scheduled'] === 1) {
            tick = 'mdi-checkbox-marked-outline';
          }
          html_snippet += `
          <tr>
            <td><a href="/projects/` + project['_id'] + `">` + project['name'] + `</a></td>
            <td> ` + project['user'] + ` </td>
            <td> <label class="badge ` + badge + `">` + status + `</label> </td>
            <td> ` + project['timestamp'] + ` </td>
            <td><span><i class="mdi ` + tick + `"></i></span></td>
          </tr>`;
        }
      }
      $(update_id).html(html_snippet);
    })
}

function getNotifications() {
  let uri = url + 'api/notifications/';
  fetch(uri)
    .then(response => response.json())
    .then(data => {
      let html_snippet = '';
      if (Object.keys(data).length > 0) {
        $('#has-notifications').removeClass('d-none');
        html_snippet += '<h6 class="p-3 mb-0">Notifications</h6>';
      } else {
        $('#has-notifications').addClass('d-none');
      }
      for (let n_timestamp of Object.keys(data).reverse()) {
        if (data.hasOwnProperty(n_timestamp)) {
          let notification = data[n_timestamp];
          /**
           * Critical - danger - 0
           * Warning - warning - 1
           * Info - info - 2
           * Debug - primary - 3
          **/
          let preview_icon = 'mdi-alert-circle-outline';
          let level = 'bg-danger'
          switch (notification['level']) {
            case 0:
              preview_icon = 'mdi-radioactive';
              level = 'bg-danger';
              break;
            case 1:
              preview_icon = 'mdi-alert-circle-outline';
              level = 'bg-warning';
              break
            case 2:
              preview_icon = 'mdi-rss';
              level = 'bg-info';
              break
            case 3:
              preview_icon = 'mdi-flask-outline';
              level = 'bg-primary';
              break
          }
          html_snippet += `
          <div class="dropdown-divider"></div>
            <a class="dropdown-item preview-item" onclick="dismissNotification('`+ notification['_id'] +`')">
              <div class="preview-thumbnail">
                <div class="preview-icon `+ level +`">
                    <i class="mdi `+ preview_icon +`"></i>
                </div>
              </div>
              <div class="preview-item-content d-flex align-items-start flex-column justify-content-center">
                <h6 class="preview-subject font-weight-normal mb-1">`+ notification['title'] +`</h6>
                <p class="text-gray ellipsis mb-0">`+ notification['info'] +`</p>
              </div>
            </a>
          `;
        }
      }
      $('#notification-list').html(html_snippet);
    })
}

function dismissNotification(id) {
  let uri = url + 'api/notifications/' + id + '/dismiss';
  fetch(uri)
    .then(response => response.json())
    .then(data => {
      console.log(data);
      getNotifications();
    })
}

var intervalID = window.setInterval(function () {
  getNotifications();
}, 10000)

document.addEventListener("visibilitychange", function() {
	if (!document.hidden) {
    window.clearInterval(intervalID);
    intervalID = window.setInterval(function () {
      getNotifications();
    }, 10000)
  } else {
    window.clearInterval(intervalID);
  }
});
