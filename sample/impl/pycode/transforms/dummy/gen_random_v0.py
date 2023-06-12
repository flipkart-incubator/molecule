# %%%
# name: GetRandomV0
# type: generate_random_number
# affinity: cpu
# version: 0.0.1
# inputs:
# outputs:
# 	new_str:
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset
import random

class GetRandomV0(TransformBase):
  def apply(self):
    random.seed(self.params['seed'])
    new_str = str(random.randint(1000, 9999))
    self.outputs['new_str'] = Dataset(value=new_str)
