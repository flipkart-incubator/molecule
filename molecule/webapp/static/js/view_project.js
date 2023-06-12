window.status = window.data['status'];

function generateId() {
  return Math.random().toString(36).substr(2) + Math.random().toString(36).substr(2);
}

$('.status-btn').click(function () {
  let status = parseInt($(this).attr('id').substr(14));
  $('.status-btn').removeClass(function (index, className) {
    return (className.match(/(^|\s)btn-gradient-\S+/g) || []).join(' ');
  }).attr('disabled', false);
  projectStatusUpdate(status);
})


function projectStatusUpdate(status) {
  window.status = status;
  switch (status) {
    case 0:
      $('#status-button-0')
        .addClass('btn-gradient-dark')
        .attr('disabled', true);
      break;
    case 1:
      $('#status-button-1')
        .addClass('btn-gradient-primary')
        .attr('disabled', true);
      break;
    case 2:
      $('#status-button-2')
        .addClass('btn-gradient-info')
        .attr('disabled', true);
      break;
    case 3:
      $('#status-button-3')
        .addClass('btn-gradient-warning')
        .attr('disabled', true);
      break;
    case 4:
      $('#status-button-4')
        .addClass('btn-gradient-success')
        .attr('disabled', true);
      break;
    case 5:
      $('#status-button-5')
        .addClass('btn-gradient-secondary')
        .attr('disabled', true);
      break;
    case 6:
      $('#status-button-6')
        .addClass('btn-gradient-danger')
        .attr('disabled', true);
      break;
  }
}

function addProject() {
  let uri = url + 'api/projects/update'
  let p_id = window.data['_id'];
  let form_data = {
    _id: p_id === '' ? generateId() : p_id,
    name: $('#project-name').val().replace(/\s+/g, ''),
    user: $('#project-user').val(),
    timestamp: $('#project-created-at').val(),
    status: parseInt(window.status),
    scheduled: parseInt(window.data['scheduled']),
    runs: window.data['runs'],
    notes: $('#project-notes').val(),
    working_dir: $('#project-working-dir').val(),
    pipeline_name: $('#project-pipeline-name').val(),
    pipeline_spec_loc: $('#project-pipeline-spec-loc').val(),
    pipeline_config: $('#project-pipeline-config').val(),
    update_config: $('#update-config').is(':checked') === true ? 1 : 0
  }
  console.log(form_data);
  $.post(uri, form_data).done(function (data) {
    console.log(data);
    $('#update-alert').removeClass('d-none');
    setInterval(function () {
      $('#update-alert').addClass('d-none');
    }, 5000);
    setInterval(function () {
      if (p_id === '')
        window.location.pathname = '/projects';
    }, 5000);
  })
}

function deleteProject() {
  let uri = url + 'api/projects/' + window.data['_id'] + '/delete';
  fetch(uri)
    .then(response => response.json())
    .then(data => {
      console.log(data);
      window.location.pathname = '/projects';
    })
}

$('#close-alert').click(function () {
  $('#update-alert').addClass('d-none');
})

window.onload = function () {
  updatePage();
}

function updatePage() {
  projectStatusUpdate(window.data['status']);
  populatePipelines(window.pipelines, '#project-pipelines')
}
