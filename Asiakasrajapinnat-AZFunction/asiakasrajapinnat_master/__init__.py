"""Timer triggered pipeline that processes and exports customer data."""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List
import traceback

import pytz
import azure.functions as func
from azure.storage.blob import ContentSettings

from .customer import Customer
from .data_builder import DataBuilder
from .data_editor import DataEditor
from .main_config import MainConfig
from .storage_handler import StorageHandler

# version 1.24


def get_timestamp(strftime: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Return the current timestamp in the format 'YYYY-MM-DD_HH%M'
    in Finland timezone.
    """
    finland_tz = pytz.timezone('Europe/Helsinki')
    finland_time = datetime.now(finland_tz)
    return finland_time.strftime(strftime)


def load_customers_from_config(
    base_columns: Dict[str, Dict[str, str]],
    storage: StorageHandler) -> List[Customer]:
    """Read all customer JSON configs and instantiate ``Customer`` objects."""
    customers: List[Customer] = []
    for cfg_file in storage.list_json_blobs(prefix="CustomerConfig/"):
        json_data = storage.download_blob(cfg_file)
        data = json.loads(json_data)
        customer = Customer(**data, base_columns=base_columns)
        customers.append(customer)
    return customers


def process_customer(customer: Customer, src_stg: StorageHandler) -> None:
    """Process a single customer and upload the resulting file."""
    if not customer.enabled:
        logging.info("Skipping customer %s as it is not enabled.", customer.name)
        return

    logging.info("Processing customer %s...", customer.name)

    stg_prefix = "Rajapinta/" + customer.source_container
    if not stg_prefix:
        logging.info(
            "Source container for customer %s is empty.", customer.name
        )
        return

    df = customer.get_data(src_stg, stg_prefix)
    if df.empty:
        logging.info("No data found for customer %s.", customer.name)
        return

    editor = DataEditor(df=df, customer=customer)
    df_final = (
        editor.delete_row(0)
        .validate_concern_number()
        .drop_unmapped_columns()
        .reorder_columns()
        .cast_and_round()
        .validate_final_df()
        .df
    )

    ts = get_timestamp(strftime="%Y-%m-%d_%H-%M-%S")

    builder = DataBuilder(customer)
    if customer.file_format.lower() == "csv":
        data = builder.build_csv(df_final, encoding=customer.file_encoding)
        blob_name = f"tapahtumat_{customer.name}_{ts}.csv"
        content_settings = ContentSettings(
            content_type=f"text/csv; charset={customer.file_encoding}"
        )
    elif customer.file_format.lower() == "json":
        data = builder.build_json(df_final)
        blob_name = f"tapahtumat_{customer.name}_{ts}.json"
        content_settings = ContentSettings(
            content_type=f"application/octet-stream; charset={customer.file_encoding}"
        )
    else:
        raise ValueError(f"Invalid file format: {customer.file_format}")

    dst_stg = StorageHandler(customer.destination_container, verify_existence=True)
    dst_stg.upload_blob(blob_name, data, content_settings=content_settings)
    logging.info("Processed customer %s successfully.", customer.name)


def main() -> None:
    """Entry point for the timer triggered function."""
    try:
        logging.basicConfig(level=logging.INFO)
        logging.info("Process started at %s.", get_timestamp())
        start_time = time.perf_counter()

        conf_stg = StorageHandler(
            container_name="asiakasrajapinnat", verify_existence=True
        )
        src_stg = StorageHandler(
            container_name="vitecpowerbi", verify_existence=True
        )

        maincfg = MainConfig(conf_stg)

        customers = load_customers_from_config(maincfg.base_columns, conf_stg)
        logging.info("Loaded %d customers from config.", len(customers))

        for customer in customers:
            try:
                process_customer(customer, src_stg)
            except ValueError as err:
                logging.error(
                    "Error processing customer %s: %s", customer.name, err
                )

        elapsed_time = time.perf_counter() - start_time

        logging.info("Process completed successfully at %s.", get_timestamp())
        logging.info("Elapsed time: %.2f seconds.", elapsed_time)
    except Exception:
        # ----------------------------------------
        # 5. FAILURE: OUTPUT LOG + STACK TRACE
        # ----------------------------------------
        logging.error("Unexpected error occurred: %s", traceback.format_exc())

        # Re-raise the exception to ensure the function is marked as failed
        raise
