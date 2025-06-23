import pandas as pd
import pytest

from asiakasrajapinnat_master.esrs_data_parser import EsrsDataParser


def test_parse_requires_columns():
    df = pd.DataFrame({'A': [1]})
    parser = EsrsDataParser(df)
    with pytest.raises(ValueError):
        parser.parse()