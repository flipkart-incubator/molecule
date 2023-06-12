# %%%
# name: InferUSAHousing
# type: infer_usa_housing
# affinity: cpu
# version: 0.2.1
# inputs:
#   data_var:
#     test_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
#     train_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
#   model_var:
# outputs:
#   out_var:
#     test_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price, price_pred
#     train_data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price
# desc:
# %%%

from molecule.executors.py.transform_base import TransformBase, Dataset


class InferUSAHousing(TransformBase):
  def apply(self):
    y_col = self.params['y_col']
    train_data = self.inputs['data_var'].train_data
    test_data = self.inputs['data_var'].test_data
    model = self.inputs['model_var'].model

    X_test = test_data.drop(y_col, axis=1)
    y_test = test_data[[y_col]]

    pred = model.predict(X_test)

    test_data[y_col + '_pred'] = pred

    self.outputs['out_var'] = Dataset(
        train_data=train_data,
        test_data=test_data
    )
