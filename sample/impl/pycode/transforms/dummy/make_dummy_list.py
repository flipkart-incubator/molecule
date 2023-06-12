# %%%
# name: MakeDummyList
# type: make_dummy_list
# affinity: cpu
# version: 0.0.1
# inputs:
# 	cut_str:
# outputs:
# 	str_list:
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset

class MakeDummyList(TransformBase):
  def apply(self):
    cut_str = self.inputs['cut_str'].value
    repeat = self.params['repeat']
    str_list = [cut_str] * repeat
    self.logger.info('test logging')
    self.outputs['str_list'] = Dataset(value=str_list)
