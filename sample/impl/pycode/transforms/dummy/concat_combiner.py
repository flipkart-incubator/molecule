# %%%
# name: DummyConcatCombiner
# type: dummy_combiner
# affinity: cpu
# version: 0.0.1
# inputs:
# 	collect_strs:
# outputs:
# 	comb_out:
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset

class DummyConcatCombiner(TransformBase):
  def apply(self):
    comb_out = '|'
    for inp in self.inputs['collect_strs'].values():
      comb_out += inp.value + '|'
    self.outputs['comb_out'] = Dataset(value=comb_out)
