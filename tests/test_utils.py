import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from isaric_to_omop.utils import merge_columns_with_postfixes


@pytest.mark.parametrize("df,expected",
                         [(pd.DataFrame(data={"ethnic": [np.nan, np.nan, np.nan],
                                              "ethnic___2": [np.nan, "1", np.nan],
                                              "ethnic___10": [1, np.nan, 1]}),
                           pd.DataFrame(data={"ethnic": ["10", "2", "10"]})
                           )])
# todo add more params
def test_merge_cols_with_postfix(df, expected):
    actual = merge_columns_with_postfixes(df, "ethnic")
    assert_frame_equal(actual, expected)
