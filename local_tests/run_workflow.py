"""Run the asiakasrajapinnat_master workflow using local example data."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import logging

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from asiakasrajapinnat_master.customer import Customer, CustomerConfig
from asiakasrajapinnat_master.data_builder import DataBuilder
from asiakasrajapinnat_master.data_editor import DataEditor
from asiakasrajapinnat_master.esrs_data_parser import EsrsDataParser
from asiakasrajapinnat_master.database_handler import DatabaseHandler




logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

ROOT = Path(__file__).resolve().parents[1]


def load_configs() -> tuple[Customer, dict]:
    """Load main and customer configs from the ``Config`` folder."""
    with open(ROOT / "Config" / "MainConfig.json", encoding="utf-8") as fh:
        main_cfg = json.load(fh)

    with open(ROOT / "Config" / "test.json", encoding="utf-8") as fh:
        cust_cfg_raw = json.load(fh)

    cfg = CustomerConfig(base_columns=main_cfg["base_columns"], **cust_cfg_raw)
    return Customer(cfg), main_cfg["base_columns"]


def read_local_settings() -> dict:
    """Read local.settings.json to get environment variables."""
    settings_path = ROOT / "local.settings.json"
    if not settings_path.exists():
        raise FileNotFoundError(
            f"Local settings file not found: {settings_path}")

    with open(settings_path, encoding="utf-8") as fh:
        return json.load(fh).get("Values", {})


def filter_month(df, month: int, year: int = 2025, exclude: bool = False) -> pd.DataFrame:
    # 1) Parse your Pvm column (if you haven’t already):
    df['Pvm_dt'] = pd.to_datetime(
        df['Pvm'],
        dayfirst=True,      # since your dates are "DD.MM.YYYY"
        format='%d.%m.%Y',
        errors='coerce'
    )

    # 2) Choose the month you want (1 = Jan, …, 12 = Dec)
    selected_month = month

    # 3) Filter to year == 2025 AND month == selected_month
    if not exclude:
        df_filtered = df[
            (df['Pvm_dt'].dt.year == year) &
            (df['Pvm_dt'].dt.month == selected_month)
        ].copy()
    else:
        df_filtered = df[
            ((df['Pvm_dt'].dt.year != year) &
             (df['Pvm_dt'].dt.month != selected_month))
        ].copy()

    # drop the helper datetime column
    df_filtered.drop(columns='Pvm_dt', inplace=True)

    return df_filtered


def run():
    """Process the example data and save the result to the repository root."""
    customer, base_columns = load_configs()

    local_settings = read_local_settings()
    os.environ.update(local_settings)

    results_dir = ROOT / "local_tests" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Load the sample CSV
    sample_path = ROOT / "local_tests" / "Rajapinta_newest_malli.csv"
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
        .rename_and_cast_datatypes()
        .format_date_and_time()
        .normalize_null_values()
        .validate_final_df()
        .df
    )

    #df_final = filter_month(df_final, month=12, year=2024, exclude=True)

    db = DatabaseHandler(base_columns=base_columns, local_test=True)
    db.upsert_rows(customer.config.name, df_final)
    full_df = db.fetch_dataframe(customer.config.name)

    out_path = results_dir / f"TESTST.csv"
    out_path.write_text(full_df.to_csv(
        index=False, encoding=customer.config.file_encoding, sep=";"), encoding=customer.config.file_encoding)

    builder = DataBuilder(customer)
    if customer.config.file_format.lower() == "csv":
        data = builder.build_csv(
            df_final, encoding=customer.config.file_encoding)
        out_path = results_dir / f"tapahtumat_{customer.config.name}.csv"
        out_path.write_text(data, encoding=customer.config.file_encoding)
    else:
        data = builder.build_json(df_final)
        out_path = results_dir / f"tapahtumat_{customer.config.name}.json"
        out_path.write_text(data, encoding=customer.config.file_encoding)

    esrs_parser = EsrsDataParser(full_df)
    json_data = esrs_parser.parse()
    json_out_path = results_dir / f"esrs_{customer.config.name}.json"
    json_out_path.write_text(json.dumps(
        json_data, indent=4), encoding=customer.config.file_encoding)

    return


if __name__ == "__main__":
    run()
