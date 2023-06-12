import logging
import traceback

class Dataset:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class TransformBase:
  def __init__(self, inputs, params, outputs=None, context=None):
    if outputs is None:
      outputs = dict()
    self.inputs = inputs
    self.params = params
    self.outputs = outputs
    self.context = context
    if context is None:
      logging.basicConfig(level=logging.DEBUG)
      self.logger = logging
    else:
      self.logger = context.logger
      self.inputs_path, self.outputs_path = context.paths

  def apply(self):
    raise Exception("sub class doesn't support apply call")

  def run(self):
    try:
      self.apply()
    except Exception as err:
      self.logger.critical(str(self.__class__.__name__) + " failed to apply, please check and run again")
      self.logger.critical(err)
      self.logger.critical(traceback.format_exc())
      exit(1)
