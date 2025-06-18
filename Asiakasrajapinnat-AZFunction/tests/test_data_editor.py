from asiakasrajapinnat_master.data_editor import DataEditor
from asiakasrajapinnat_master.customer import Customer, CustomerConfig
import os
import sys
import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "Asiakasrajapinnat-AZFunction")))


def make_editor(invalid=False):
    base_cols = {
        "PARConcern": {"name": "Konserninumero", "dtype": "string"},
        "A": {"name": "ValueA", "dtype": "float"},
        "B": {"name": "ValueB", "dtype": "string"},
    }
    cfg = CustomerConfig(
        name="test",
        konserni={100, 200},
        source_container="src/",
        destination_container="dest/",
        file_format="csv",
        file_encoding="utf-8",
        extra_columns=None,
        enabled=True,
        base_columns=base_cols,
    )
    customer = Customer(cfg)
    df = pd.DataFrame({
        "PARConcern": ["100", "100" if not invalid else "999", "200"],
        "A": ["1,5", "2,5", "3,5"],
        "B": ["x", "y", "z"],
        "Unused": [1, 2, 3],
    })
    return DataEditor(df, customer)


def test_data_editor_processing():
    editor = make_editor()
    final = (
        editor.delete_row(0)
        .validate_concern_number()
        .drop_unmapped_columns()
        .reorder_columns()
        .rename_and_cast_datatypes()
        .validate_final_df()
        .df
    )
    expected = pd.DataFrame({
        "Konserninumero": ["100", "200"],
        "ValueA": [2.5, 3.5],
        "ValueB": ["y", "z"],
    })
    pd.testing.assert_frame_equal(final.reset_index(drop=True), expected)


def test_validate_concern_number_fails():
    editor = make_editor(invalid=True)
    editor.delete_row(0)
    with pytest.raises(ValueError):
        editor.validate_concern_number()
