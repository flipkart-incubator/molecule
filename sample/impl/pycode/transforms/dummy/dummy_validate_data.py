# %%%
# name: DummyValidateData
# type: dummy_data_import
# affinity: cpu
# version: 0.0.1
# inputs:
#   hive_raw:
#     hive_raw:
# outputs:
#   validated_data:
#     data:
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset
import pandas as pd
import random

class DummyValidateData(TransformBase):
  def apply(self):
    hive_raw = self.inputs['hive_raw'].hive_raw
    validated_data = []
    delete_hashes = []
    if len([*pd.unique(hive_raw['super_category'])]) == 1 and [*pd.unique(hive_raw['super_category'])] == [self.params['sc']]:
      validated_data = hive_raw
    else:
      delete_hashes = ['hive_raw']
    self.outputs['validated_data'] = Dataset(
      data=validated_data,
      sc=self.params['sc']
    )
    self.outputs['delete_hashes'] = delete_hashes
