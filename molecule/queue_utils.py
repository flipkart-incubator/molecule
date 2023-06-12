class MultiProcessPriorityQueue:
  """
  A class that implements a priority queue with three levels of priority (0, 1, 2) and thread-safe operations.
  """
  def __init__(self, manager):
    """
    Initializes the MultiProcessPriorityQueue object with three lists for each priority level and a lock object.
    """
    self.lock = manager.Lock()
    self.p0 = manager.list()
    self.p1 = manager.list()
    self.p2 = manager.list()
    
  def flatten(self):
    """
    Returns a flattened list of all elements in the priority queue.
    """
    self.lock.acquire()
    flattened_list = [*self.p0, *self.p1, *self.p2]
    self.lock.release()
    return flattened_list
    
  def appendToQueue(self, Obj, priority):
    """
    Adds an object to the priority queue with the specified priority level.
    """
    self.lock.acquire()
    if priority == 0:
      self.p0.append(Obj)
    elif priority == 1:
      self.p1.append(Obj)
    elif priority == 2:
      self.p2.append(Obj)
    self.lock.release()
  
  def getLen(self, priority=None):
    """
    Returns the length of the priority queue or the length of a specific priority level.
    """
    total_len = 0
    self.lock.acquire()
    if priority is None:
      total_len = len(self.p0) + len(self.p1) + len(self.p2)
    else:
      if priority == 0:
        total_len = len(self.p0)
      elif priority == 1:
        total_len = len(self.p1)
      elif priority == 2:
        total_len = len(self.p2)
    self.lock.release()
    return total_len
  
  def popFromPriorityQueue(self, priority):
    """
    Removes and returns the first element from the priority queue with the specified priority level.
    """
    ele = None
    self.lock.acquire()
    if priority == 0 and len(self.p0) > 0:
      ele = self.p0.pop(0)
    if priority == 1 and len(self.p1) > 0:
      ele = self.p1.pop(0)
    if priority == 2 and len(self.p2) > 0:
      ele = self.p2.pop(0)
    self.lock.release()
    return ele
  
  def popFromQueue(self):
    """
    Removes and returns the first element from the priority queue with the highest priority level.
    """
    ele = None
    priority = None
    self.lock.acquire()
    if len(self.p0) > 0:
      ele = self.p0.pop(0)
      priority = 0
    elif len(self.p1) > 0:
      ele = self.p1.pop(0)
      priority = 1
    elif len(self.p2) > 0:
      ele = self.p2.pop(0)
      priority = 2
    self.lock.release()
    return ele, priority 
  
  def checkInQueue(self, Obj):
    """
    Returns True if the object is present in the priority queue, False otherwise.
    """
    self.lock.acquire()
    present_in_queue = False
    if Obj in self.p0 or Obj in self.p1 or Obj in self.p2:
      present_in_queue=True
    self.lock.release()
    return present_in_queue

class QueueUtils:
  """
  A class that implements thread-safe operations on a queue and a dictionary.
  """
  def __init__(self, lock):
    """
    Initializes the QueueUtils object with a lock object.
    """
    self.lock = lock

  def checkInQueue(self, Q, Obj):
    """
    Returns True if the object is present in the queue, False otherwise.
    """
    self.lock.acquire()
    present_in_queue = True if Obj in Q else False
    self.lock.release()
    return present_in_queue

  def appendToQueue(self, Q, Obj):
    """
    Adds an object to the queue.
    """
    self.lock.acquire()
    Q.append(Obj)
    self.lock.release()

  def popFromQueue(self, Q):
    """
    Removes and returns the first element from the queue.
    """
    self.lock.acquire()
    if len(Q) > 0:
      ele = Q.pop(0)
      self.lock.release()
      return ele
    else:
      self.lock.release()
      return None

  def removeFromQueue(self, Q, Obj):
    """
    Removes an object from the queue.
    """
    self.lock.acquire()
    Q.remove(Obj)
    self.lock.release()

  def addToMap(self, M, Key, Value):
    """
    Adds a key-value pair to the dictionary.
    """
    self.lock.acquire()
    M[Key] = Value
    self.lock.release()

  def deleteFromMap(self, M, Key):
    """
    Removes a key-value pair from the dictionary.
    """
    self.lock.acquire()
    del M[Key]
    self.lock.release()
