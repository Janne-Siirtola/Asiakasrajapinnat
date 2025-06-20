"""Run the asiakasrajapinnat_master workflow using local example data."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from asiakasrajapinnat_master.customer import Customer, CustomerConfig
from asiakasrajapinnat_master.data_builder import DataBuilder
from asiakasrajapinnat_master.data_editor import DataEditor
from asiakasrajapinnat_master.esrs_data_parser import EsrsDataParser


ROOT = Path(__file__).resolve().parents[1]


def load_configs() -> Customer:
    """Load main and customer configs from the ``Config`` folder."""
    with open(ROOT / "Config" / "MainConfig.json", encoding="utf-8") as fh:
        main_cfg = json.load(fh)

    with open(ROOT / "Config" / "test.json", encoding="utf-8") as fh:
        cust_cfg_raw = json.load(fh)

    cfg = CustomerConfig(base_columns=main_cfg["base_columns"], **cust_cfg_raw)
    return Customer(cfg)


def run() -> Path:
    """Process the example data and save the result to the repository root."""
    customer = load_configs()

    # Load the sample CSV
    sample_path = ROOT / "local_tests" / "esrs_sample2.csv"
    df = pd.read_csv(sample_path, encoding="ISO-8859-1", delimiter=";")
    # Ensure konserni column is loaded as strings
    if "PARConcern" in df.columns:
        df["PARConcern"] = df["PARConcern"].astype(str)

    # Replicate the steps from ``process_customer``
    editor = DataEditor(df=df, customer=customer)
    df_final = (
        editor.delete_row(0)
        .validate_concern_number()
        .drop_unmapped_columns()
        .reorder_columns()
        .rename_columns()
        .cast_datatypes()
        .validate_final_df()
        .df
    )

    builder = DataBuilder(customer)
    if customer.config.file_format.lower() == "csv":
        data = builder.build_csv(df_final, encoding=customer.config.file_encoding)
        out_path = ROOT / "local_tests" / "results" / f"tapahtumat_{customer.config.name}.csv"
        out_path.write_text(data, encoding=customer.config.file_encoding)
    else:
        data = builder.build_json(df_final)
        out_path = ROOT / "local_tests" / "results" / f"tapahtumat_{customer.config.name}.json"
        out_path.write_text(data, encoding=customer.config.file_encoding)
        
    esrs_parser = EsrsDataParser(df_final)
    json_data = esrs_parser.parse()
    json_out_path = ROOT / "local_tests" / "results" / f"esrstst_{customer.config.name}.json"
    json_out_path.write_text(json.dumps(json_data, indent=4), encoding=customer.config.file_encoding)


    """ csv_data = esrs_parser.data_to_csv(json_data, encoding=customer.config.file_encoding)
    out_path = ROOT / "local_tests" / "results" / f"esrs_{customer.config.name}.csv"
    out_path.write_text(csv_data, encoding=customer.config.file_encoding) """
    return out_path


if __name__ == "__main__":
    output = run()
    print(f"Result written to {output}")
