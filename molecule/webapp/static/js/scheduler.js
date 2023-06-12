$('#project-select').change(function () {
    $('#schedule-str').val(window.data[$(this).val()]['schedule_str']);
    $('#project-command').val(window.data[$(this).val()]['command']);
})

function runProject() {
    let p_id = $('#project-select').val();
    if (p_id === null) return ;
    let uri = url + 'api/projects/' + p_id + '/run';
    fetch(uri)
        .then(response => response.json())
        .then(data => {
            console.log(data);
            $('#test-run-text').removeClass('d-none');
            $('#stdout-data').text(data['stdout']);
            $('#stderr-data').text(data['stderr']);
            $('#alert-text').text(data['status']);
            $('#update-alert').removeClass('d-none');
            setInterval(function () {
            $('#update-alert').addClass('d-none');
            }, 5000);
        })
}

function scheduleProject() {
    let p_id = $('#project-select').val();
    if (p_id === null) return ;
    let schedule_str = $('#schedule-str').val().trim();
    let uri = url + 'api/projects/' + p_id + '/schedule';
    $.post(uri, {'schedule_str': schedule_str}).done(function (data) {
        console.log(data);
        $('#alert-text').text(data['status']);
        $('#update-alert').removeClass('d-none');
        setInterval(function () {
          $('#update-alert').addClass('d-none');
        }, 5000);
    })
}

function unscheduleProject() {
    let p_id = $('#project-select').val();
    if (p_id === null) return ;
    let uri = url + 'api/projects/' + p_id + '/unschedule';
    fetch(uri)
        .then(response => response.json())
        .then(data => {
            console.log(data);
            $('#alert-text').text(data['status']);
            $('#update-alert').removeClass('d-none');
        setInterval(function () {
          $('#update-alert').addClass('d-none');
        }, 5000);
        })
}
