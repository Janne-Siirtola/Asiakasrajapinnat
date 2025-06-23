import pandas as pd
from asiakasrajapinnat_master.data_builder import DataBuilder
from asiakasrajapinnat_master.customer import Customer, CustomerConfig


def _make_customer():
    cfg = CustomerConfig(
        name="test",
        konserni=set(),
        source_container="src/",
        destination_container="dst/",
        file_format="csv",
        file_encoding="utf-8",
        extra_columns=None,
        enabled=True,
        base_columns={
            "A": {"name": "A", "dtype": "int"},
            "B": {"name": "B", "dtype": "int"},
        },
    )
    return Customer(cfg)


def test_build_csv_uses_unix_newlines():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    cust = _make_customer()
    builder = DataBuilder(cust)
    csv = builder.build_csv(df, encoding="utf-8")
    # Should only have as many newline characters as rows + header
    assert csv.count("\n") == len(df) + 1
    assert "\r\n" not in csv
