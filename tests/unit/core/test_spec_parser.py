from unittest import TestCase
from molecule.spec_parser import SpecParser
import sys


class TestSpecParser(TestCase):

  def setUp(self) -> None:
    self.current_module = sys.modules[__name__]
    self.sample_t_spec = {
      "params": {
        "sc": "str",
        "delete_event_days": "bool"
      },
      "inputs": {
        "sales": {
          "type": "generic_sales",
          "serde_def": None,
          "map": {
            "sc": "params/sc",
            "bu": "params/bu"
          }
        }
      },
      "outputs": {
        "hist_data": {
          "type": "sales",
          "map": {
            "sc": "params/sc"
          }
        }
      },
      "scratch": {
        "tmp_data": {
          "type": "df"
        }
      }
    }
    self.mock_base_class = type('sample', (), {})
    self.sample_t_s = {
      "merge_sales_event_cal": {
        "type": "merge_sales_event_cal",
        "params": {
          "bu": "params/bu",
          "sc": "params/sc"
        }
      },
      "train_test_split": {
        "type": "train_test_split",
        "params": {
          "fgp": "params/fgp",
          "lead_time": "params/fcstLead",
          "horizon": "params/fcstHorizon"
        }
      },
      "fe_generic_sales": {
        "type": "fe_generic_sales",
        "params": {
          "bu": "params/bu",
          "sc": "params/sc"
        }
      }
    }

  def test_create_transform_type(self):
    SpecParser.createTransformType(t_name='sample_t', t_spec=self.sample_t_spec,
                                   t_module=self.current_module, t_base=self.mock_base_class,
                                   t_expected_fields=['params', 'inputs', 'outputs'])
    self.assertFalse(not hasattr(self.current_module, 'sample_tSpec'),
                     msg=f'class sample_tSpec does not exist in {self.current_module}')

