# %%%
# name: PreprocessUSAHousing
# type: preprocess_usa_housing
# affinity: cpu
# version: 0.1.0
# inputs:
#   in_var:
#     test_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
#     train_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
# outputs:
#   out_var:
#     test_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
#     train_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler

class PreprocessUSAHousing(TransformBase):
  def apply(self):
    y_col = self.params['y_col']
    scaling_method = self.params['scaling_method']
    train_data = self.inputs['in_var'].train_data
    test_data = self.inputs['in_var'].test_data

    scale_cols = train_data.columns.drop(y_col).to_list()
    self.logger.info('Columns to scale: ')
    self.logger.info(scale_cols)

    if scaling_method == 'standard':
      scaler = StandardScaler()
    elif scaling_method == 'robust':
      scaler = RobustScaler()
    elif scaling_method == 'minmax':
      scaler = RobustScaler()
    else:
      scaler = None

    if scaler is not None:
      train_scaled_features = scaler.fit_transform(train_data[scale_cols].values)
      train_data[scale_cols] = train_scaled_features
      test_scaled_features = scaler.transform(test_data[scale_cols].values)
      test_data[scale_cols] = test_scaled_features
      self.logger.info('Preprocessing Data Complete')
    else:
      self.logger.warning('No scaling applied')

    self.outputs['out_var'] = Dataset(
        train_data=train_data,
        test_data=test_data
    )
