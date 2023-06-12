# %%%
# name: ComputeMetricUSAHousing
# type: compute_metrics_usa_housing
# affinity: cpu
# version: 0.6.0
# inputs:
#   in_var:
#     test_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price, price_pred
#     train_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
# outputs:
#   out_var:
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset
import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.model_selection import cross_val_score


class ComputeMetricUSAHousing(TransformBase):
  def apply(self):
    y_col = self.params['y_col']
    train_data = self.inputs['in_var'].train_data
    test_data = self.inputs['in_var'].test_data

    true_y = test_data[[y_col]]
    predicted_y = test_data[[y_col + '_pred']]

    mae = metrics.mean_absolute_error(true_y, predicted_y)
    mse = metrics.mean_squared_error(true_y, predicted_y)
    rmse = np.sqrt(metrics.mean_squared_error(true_y, predicted_y))
    r2_square = metrics.r2_score(true_y, predicted_y)

    self.logger.info(pd.DataFrame(data=[["Linear Regression", mae, mse, rmse, r2_square]],
                                 columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square']))

    self.outputs['out_var'] = Dataset(
        mae=mae,
        mse=mse,
        rmse=rmse,
        r2=r2_square
    )
