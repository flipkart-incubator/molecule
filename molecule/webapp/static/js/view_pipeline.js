window.r_data = window.data;

function getPipelineData() {
  $('#view-log').attr('href', '/api/logs/molecule/server')
  $('#view-task-log').attr('href', '#')
  window.r_data = window.data;
  updatePage();
}

function updatePage() {
  let formatter = new JSONFormatter(window.r_data, 1)
  $('#pipeline-data').html(formatter.render());
  window.graph = getLayout(window.data['graph']);
  if (window.s === undefined)
    plotDAG(window.graph);
  updateGraph(window.graph).then(() => {
    window.s.refresh();
  })
}

function getLayout(graph) {
  let g = new dagre.graphlib.Graph();

  g.setGraph({});

  for (let node of graph['nodes']) {
    g.setNode(node['id'], {label: node['label'], width: node['size'], height: node['size']});
  }

  for (let edge of graph['edges']) {
    g.setEdge(edge['source'], edge['target'], {label: edge['id']});
  }

  dagre.layout(g);

  let container = $('#container');
  if (container.width() < g.graph().width) {
    container.width(g.graph().width);
  }
  container.height(g.graph().height*2);

  for (let i in graph['nodes']) {
    graph['nodes'][i]['x'] = g.node(graph['nodes'][i]['id']).x;
    graph['nodes'][i]['y'] = g.node(graph['nodes'][i]['id']).y;
  }

  return(graph);
}

function getStatusDetails(status) {
  switch (status) {
    case 0: // queued
      return ['#198ae3', 10];
    case 1: // processing
      return ['#b66dff', 25];
    case 2: // completed
      return ['#1bcfb4', 20];
    case 3: // terminated
      return ['#fe7c96', 15];
    case 4: // stale
      return ['#c3bdbd', 10];
    default: // stale
      return ['#c3bdbd', 10]
  }
}

async function updateGraph(graph) {
  let fetchArray = [];
  for (let i in graph['nodes']) {
    let node = graph['nodes'][i];
    let t_hash = node['id'];
    let uri = url + 'api/tasks/' + t_hash + '/status';
    fetchArray.push(
      fetch(uri)
        .then(response => response.json())
        .then(res_data => {
          let [nodeColor, nodeSize] = getStatusDetails(res_data['status'])
          window.s.graph.nodes(node['id']).color = nodeColor;
          window.s.graph.nodes(node['id']).size = nodeSize;
        })
    )
  }
  return await Promise.all(fetchArray);
}

function plotDAG(graph) {
  window.s = new sigma({
    graph: graph,
    container: 'container',
    settings: {
      defaultNodeColor: '#198ae3',
      sideMargin: 10,
      minNodeSize: 0,
      maxNodeSize: 0,
      minEdgeSize: 0,
      maxEdgeSize: 0,
      drawLabels: false,
      drawEdgeLabels: false,
      enableCamera: false,
      hoverFont: 'ubuntu-regular',
      labelHoverShadowColor: '#555'
    }
  });
  window.s.bind('clickNode', function (e) {
    let t_hash = e.data.node.id
    let uri = url + 'api/tasks/' + t_hash;
    fetch(uri)
      .then(response => response.json())
      .then(res_data => {
        window.r_data = res_data;
        updatePage();
        $('#view-log').attr('href', '/api/logs/instance/' + res_data['instance_id']);
        $('#view-task-log').attr('href', '/api/logs/transform/' + t_hash);
        if ($('#download-file-list').hasClass('show'))
          $('#download-file-btn').click();
      })
  })
}

function updatePipelineData() {
  let uri = url + 'api/pipelines/' + window.data.hash + '/update';
  let project_name = $('#project-select').val();
  let data = {
    message: $('#commit-message').val(),
    project: (project_name!=null) ? project_name : 'default'
  }
  $.post(uri, data).done(function (data) {
    console.log(data);
    $('#update-alert').removeClass('d-none');
    setInterval(function () {
      $('#update-alert').addClass('d-none');
    }, 5000);
  })
}

$('#close-alert').click(function () {
  $('#update-alert').addClass('d-none');
})

function getFileList(e) {
  if (!window.r_data.hasOwnProperty('hashes') ||
      !window.r_data.hashes.hasOwnProperty('outputs') ||
      window.r_data.status !== 2) {
    e.stopPropagation();
    e.preventDefault();
    e.stopImmediatePropagation();
    return false;
  }
  if ($('#download-file-list').hasClass('show'))
    return false;
  $('#download-file-list').html('');
  let uri = url + 'api/download/details';
  for (o_name of Object.keys(window.r_data.hashes.outputs)) {
    let o_hash = window.r_data.hashes.outputs[o_name];
    let data = {
      has_git: window.data.config['git_url'] === null ? false : true,
      user: window.data.user,
      o_hash: o_hash,
      o_name: o_name
    }
    $.post(uri, data).done(function (res_data) {
      console.log(res_data);
      if (res_data.status === 1) {
        let html_snippet = '';
        html_snippet += `
          <h6 class="ml-3 mb-0">` + o_name + `</h6>
          <div class="dropdown-divider"></div>
        `;
        for (filename of res_data.file_list) {
          let query = {
            has_git: res_data['has_git'],
            user: res_data['user'],
            o_hash: res_data['o_hash'],
            filename: filename
          }
          html_snippet += '<a class="dropdown-item" href="/api/download/start?' + 
          $.param(query) + '" target="_blank">' + filename + '</a>';
        }
        $('#download-file-list').append(html_snippet);
      }
    })
  }
}

window.onload = function () {
  updatePage();
}

var intervalID = window.setInterval(function () {
  updateGraph(window.graph).then(() => {
    window.s.refresh();
  });
}, 5000)

document.addEventListener("visibilitychange", function() {
	if (!document.hidden) {
    window.s.refresh();
    window.clearInterval(intervalID);
    intervalID = window.setInterval(function () {
      updateGraph(window.graph).then(() => {
        window.s.refresh();
      });
    }, 5000)
  } else {
    window.clearInterval(intervalID);
  }
});
