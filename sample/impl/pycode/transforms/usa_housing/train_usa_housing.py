# %%%
# name: TrainUSAHousing
# type: train_usa_housing
# affinity: cpu
# version: 0.3.8
# inputs: 
#   in_var: 
#     test_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
#     train_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
# outputs: 
#   out_var: 
# desc: 
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset
from sklearn.linear_model import LinearRegression
import pandas as pd
import numpy as np

class TrainUSAHousing(TransformBase):
  def apply(self):
    y_col = self.params['y_col']
    normalize = self.params['normalize']
    fit_intercept = self.params['fit_intercept']
    n_jobs = self.params['n_jobs']
    train_data = self.inputs['in_var'].train_data
    test_data = self.inputs['in_var'].test_data

    X_train = train_data.drop(y_col, axis=1)
    y_train = train_data[[y_col]]

    lin_reg = LinearRegression(fit_intercept=fit_intercept, n_jobs=n_jobs, normalize=normalize)
    lin_reg.fit(X_train, y_train)

    self.logger.info("Intercept: " + str(lin_reg.intercept_))
    self.logger.info("Coefficient Matrix: ")
    coeff = pd.DataFrame(np.array(lin_reg.coef_).flatten(), X_train.columns, columns=['Coefficient'])
    self.logger.info(coeff)

    self.outputs['out_var'] = Dataset(
        model=lin_reg
    )
