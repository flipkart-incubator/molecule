# TransformInstanceDepGraph
# TaskDepGraph
# createNode (from spec or from code)
# createEdge (from spec of from code)

class Graph:
  """
  A class representing a directed graph.

  Attributes:
  - nodes: A dictionary containing the nodes of the graph.
  - f_edges: A dictionary containing the forward edges of the graph.
  - r_edges: A dictionary containing the reverse edges of the graph.
  """
  def __init__(self):
    """
    Initializes a Graph object with empty nodes, forward edges, and reverse edges dictionaries.
    """
    self.nodes = {}
    self.f_edges = {}
    self.r_edges = {}

  def addNode(self, node_name, node_data, soft_check=False):
    """
    Adds a node to the graph.

    Args:
    - node_name: A string representing the name of the node to be added.
    - node_data: A dictionary containing the data associated with the node.
    - soft_check: A boolean indicating whether to perform a soft check for duplicate nodes.

    Returns:
    - If soft_check is True and the node already exists, returns a string indicating a duplicate node.
    - If soft_check is False and the node already exists, raises an Exception.
    - Otherwise, returns None.
    """
    if node_name in self.nodes:
      if soft_check:
        return "Duplicate Node: " + node_name
      else:
        raise Exception("Duplicate Node: %s", node_name)
    self.nodes[node_name] = node_data
    self.f_edges[node_name] = {}
    self.r_edges[node_name] = {}

  def addEdge(self, u_rhs, u_lhs, edge_data):
    """
    Adds an edge to the graph.

    """
    if self.f_edges[u_rhs].get(u_lhs, None) is None:
      self.f_edges[u_rhs][u_lhs] = []
    if self.r_edges[u_lhs].get(u_rhs, None) is None:
      self.r_edges[u_lhs][u_rhs] = []
    self.f_edges[u_rhs][u_lhs].append(edge_data)
    self.r_edges[u_lhs][u_rhs].append(edge_data)

  def addDep(self, u_lhs, u_rhs, edge_data):
    """
    Adds a dependency to the graph by adding an edge from u_rhs to u_lhs.

    """
    self.addEdge(u_rhs, u_lhs, edge_data)

  def getForwardDepsList(self, node_name):
    """
    Returns a list of all the nodes that are forward dependencies of the given node.

    Args:
    - node_name: A string representing the name of the node whose forward dependencies are to be returned.

    Returns:
    - A list of strings representing the names of all the nodes that are forward dependencies of the given node.
    """
    f_deps = []
    for ft_hash, ds_dep in self.f_edges[node_name].items():
      fft_hash = self.getForwardDepsList(ft_hash)
      f_deps = [*f_deps, ft_hash, *fft_hash]
    return list(set(f_deps))

  def toSigmaJS(self):
    """
    Returns a dictionary containing the nodes and edges of the graph in a format that can be used by SigmaJS.

    Args:
    - None

    Returns:
    - A dictionary containing the nodes and edges of the graph in a format that can be used by SigmaJS.
    """
    data = {
      'nodes': list(),
      'edges': list()
    }

    for t_hash, ti in self.nodes.items():
      data['nodes'].append({
        'id': t_hash,
        'label': ti['name'],
        'size': 10
      })

    for t_hash, ft_dict in self.f_edges.items():
      if len(ft_dict) > 0:
        for ft_hash, ds_dep in ft_dict.items():
          data['edges'].append({
            'id': t_hash+'->'+ft_hash,
            'source': t_hash,
            'target': ft_hash,
            'label': ds_dep[0],
            'size': 2.5
          })

    return data


class TopSort:
  """
  A class that performs topological sorting on a directed acyclic graph (DAG).
  """
  def __init__(self, graph):
    """
    Initializes the TopSort object with the given graph.

    Args:
    - graph: A Graph object representing the DAG to be sorted.

    Returns:
    - None
    """
    self.task_exec_order = {}
    self.task_deps = {}
    self.exec_order = []
    self.G = graph
    self.getTopSort()
    self.index = 0
    self.max = len(self.exec_order)

  def __iter__(self):
    """
    Returns an iterator object for the TopSort object.

    Args:
    - None

    Returns:
    - An iterator object for the TopSort object.
    """
    return self

  def __next__(self):
    """
    Returns the next element in the iterator.

    Args:
    - None

    Returns:
    - The next element in the iterator.

    Raises:
    - StopIteration: If there are no more elements in the iterator.
    """
    if self.index < self.max:
      res = self.exec_order[self.index]
      self.index += 1
      return res
    else:
      raise StopIteration

  def getTopSort(self):
    """
    Performs topological sorting on the DAG.

    Args:
    - None

    Returns:
    - None

    Raises:
    - Exception: If the DAG contains a cycle.
    """
    num_edges = 0  # int
    zero_deps = set()  # Set[str]
    deg_count = {}  # Dict[str, int]

    for t in self.G.nodes.keys():
      self.task_deps[t] = set()
      t_deg = len(self.G.r_edges[t])
      num_edges = num_edges + t_deg
      deg_count[t] = t_deg
      if t_deg == 0:
        zero_deps.add(t)

    while len(zero_deps) > 0:
      t = zero_deps.pop()
      self.exec_order.append(t)
      for c in self.G.f_edges[t]:
        self.task_deps[c].add(t)
        for dep in self.task_deps[t]:
          self.task_deps[c].add(dep)
        deg_count[c] = deg_count[c] - 1
        if deg_count[c] == 0:
          zero_deps.add(c)

    if len(self.exec_order) != len(self.G.nodes):
      raise Exception("No top sort. Cycle exists")

    for t in self.G.nodes:
      deps = self.task_deps[t]
      self.task_exec_order[t] = []
      for p in self.exec_order:
        if p in deps:
          self.task_exec_order[t].append(p)


def generateGraph(task_dict):
  """
  Generates a Graph object from a task dictionary.

  Args:
  - task_dict: A dictionary representing the task.

  Returns:
  - A Graph object representing the task.
  """
  g = Graph()
  
  if type(task_dict) != dict:
    task_dict = task_dict.serializeDict()

  for t_hash in task_dict['tg_list']:
    ti = task_dict['tg_dict'][t_hash]
    m = g.addNode(t_hash, ti, soft_check=True)
    if m is not None:
      print(m)

  for t_hash in task_dict['tg_list']:
    ti = task_dict['tg_dict'][t_hash]
    for out_name, out_hash in ti['hashes']['outputs'].items():
      if type(out_hash) == dict:
        for out_hash_i in out_hash.values():
          ft_list = task_dict['ds_deps'].get(out_hash_i, [])
          for ft_hash in ft_list:
            g.addDep(ft_hash, t_hash, out_hash_i)
      else:
        ft_list = task_dict['ds_deps'].get(out_hash, [])
        for ft_hash in ft_list:
          g.addDep(ft_hash, t_hash, out_hash)

  return g

