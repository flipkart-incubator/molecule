# %%%
# name: JoinToStr
# type: join_list_to_str
# affinity: cpu
# version: 0.0.1
# inputs:
# 	str_list:
# outputs:
# 	joined_str:
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset

class JoinToStr(TransformBase):
  def apply(self):
    str_list = self.inputs['str_list'].value
    joined_str = ''.join(str_list)
    self.outputs['joined_str'] = Dataset(value=joined_str)
