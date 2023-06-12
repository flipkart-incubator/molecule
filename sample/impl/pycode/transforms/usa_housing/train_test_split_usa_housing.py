# %%%
# name: TrainTestSplitUSAHousing
# type: train_test_split_usa_housing
# affinity: cpu
# version: 0.1.0
# inputs:
#   in_var:
#     data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price, address
# outputs:
#   out_var:
#     test_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
#     train_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset
from sklearn.model_selection import train_test_split


class TrainTestSplitUSAHousing(TransformBase):
  def apply(self):
    test_size = self.params['test_size']
    seed = self.params['seed']
    drop_cols = self.params['drop_cols']
    data = self.inputs['in_var'].data
    data = data.drop(drop_cols, axis=1)

    train_data, test_data = train_test_split(data, test_size=test_size, random_state=seed)

    self.outputs['out_var'] = Dataset(
        train_data=train_data,
        test_data=test_data
    )
