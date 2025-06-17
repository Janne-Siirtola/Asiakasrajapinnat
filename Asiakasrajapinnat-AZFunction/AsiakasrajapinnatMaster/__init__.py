"""Timer triggered pipeline that processes and exports customer data."""

import traceback
from .Customer import Customer
from .MainConfig import MainConfig
from .DataEditor import DataEditor
from .DataBuilder import DataBuilder
from .StorageHandler import StorageHandler
import json
from pathlib import Path
from typing import List, Dict
import logging
import pytz
from datetime import datetime
import time
import azure.functions as func
from azure.storage.blob import ContentSettings

# version 1.24


def get_timestamp(strftime: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Return the current timestamp in the format 'YYYY-MM-DD_HH%M'
    in Finland timezone.
    """
    finland_tz = pytz.timezone('Europe/Helsinki')
    finland_time = datetime.now(finland_tz)
    return finland_time.strftime(strftime)


def load_customers_from_config(base_columns: Dict[str, Dict[str, str]], storage: StorageHandler) -> List[Customer]:
    """Read all customer JSON configs and instantiate ``Customer`` objects."""
    customers: List[Customer] = []
    for cfg_file in storage.list_json_blobs(prefix="CustomerConfig/"):
        json_data = storage.download_blob(cfg_file)
        data = json.loads(json_data)
        customer = Customer(**data, base_columns=base_columns)
        customers.append(customer)
    return customers


def main(mytimer: func.TimerRequest) -> None:
    """Entry point for the timer triggered function."""
    try:
        logging.basicConfig(
            level=logging.INFO,
        )
        logging.info(f"Process started at {get_timestamp()}.")
        start_time = time.perf_counter()

        conf_stg = StorageHandler(
            container_name="asiakasrajapinnat", verify_existence=True)
        src_stg = StorageHandler(
            container_name="vitecpowerbi", verify_existence=True)

        maincfg = MainConfig(conf_stg)

        customers = load_customers_from_config(maincfg.base_columns, conf_stg)
        logging.info(f"Loaded {len(customers)} customers from config.")

        for customer in customers:
            if not customer.enabled:
                logging.info(
                    f"Skipping customer {customer.name} as it is not enabled.")
                continue

            logging.info(f"Processing customer {customer.name}...")

            stg_prefix = "Rajapinta/" + customer.source_container
            if not stg_prefix:
                logging.info(
                    f"Source container for customer {customer.name} is empty.")
                continue

            df = customer.get_data(src_stg, stg_prefix)
            if df.empty:
                logging.info(
                    f"No data found for customer {customer.name}.")
                continue

            editor = DataEditor(df=df, customer=customer)
            try:
                df_final = (editor
                            .delete_row(0)
                            .validate_concern_number()
                            .drop_unmapped_columns()
                            .reorder_columns()
                            .cast_and_round()
                            .validate_final_df()
                            .df)

                ts = get_timestamp(strftime="%Y-%m-%d_%H-%M-%S")

                # Build the data in the requested format
                data_builder = DataBuilder(customer)
                if customer.file_format.lower() == "csv":
                    data = data_builder.build_csv(
                        df_final, encoding=customer.file_encoding)
                    blob_name = f"tapahtumat_{customer.name}_{ts}.csv"
                    content_settings = ContentSettings(
                        content_type=f"text/csv; charset={customer.file_encoding}"
                    )
                elif customer.file_format.lower() == "json":
                    data = data_builder.build_json(df_final)
                    blob_name = f"tapahtumat_{customer.name}_{ts}.json"
                    content_settings = ContentSettings(
                        content_type=f"application/octet-stream; charset={customer.file_encoding}"
                    )
                else:
                    raise ValueError(
                        f"Invalid file format: {customer.file_format}")

                dst_stg = StorageHandler(
                    customer.destination_container, verify_existence=True)

                dst_stg.upload_blob(
                    blob_name, data, content_settings=content_settings)

                logging.info(
                    f"Processed customer {customer.name} successfully.")
            except ValueError as e:
                logging.error(
                    f"Error processing customer {customer.name}: {e}")
                continue

        elapsed_time = time.perf_counter() - start_time

        logging.info(f"Process completed successfully at {get_timestamp()}.")
        logging.info(f"Elapsed time: {elapsed_time:.2f} seconds.")
    except Exception as e:
        # ----------------------------------------
        # 5. FAILURE: OUTPUT LOG + STACK TRACE
        # ----------------------------------------
        logging.error("Unexpected error occurred: " + traceback.format_exc())

        # Re-raise the exception to ensure the function is marked as failed
        raise
