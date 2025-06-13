import traceback
from .Customer import Customer
from .MainConfig import MainConfig
from .DataEditor import DataEditor
from .JSONBuilder import JSONBuilder
from .StorageHandler import StorageHandler
import json
from pathlib import Path
from typing import List, Union
import logging
import pytz
from datetime import datetime
import time
import azure.functions as func

# version 1.24


def get_timestamp(strftime: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Return the current timestamp in the format 'YYYY-MM-DD_HH%M'
    in Finland timezone.
    """
    finland_tz = pytz.timezone('Europe/Helsinki')
    finland_time = datetime.now(finland_tz)
    return finland_time.strftime(strftime)


def load_customers_from_config(maincfg: MainConfig, storage: StorageHandler) -> List[Customer]:
    customers: List[Customer] = []
    for cfg_file in storage.list_json_blobs(prefix=maincfg.customer_config_path):
        json_data = storage.download_blob(cfg_file)
        data = json.loads(json_data)
        customer = Customer(**data, base_columns=maincfg.base_columns)
        customers.append(customer)
    return customers


def main(mytimer: func.TimerRequest) -> None:
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

        customers = load_customers_from_config(maincfg, conf_stg)
        logging.info(f"Loaded {len(customers)} customers from config.")

        for customer in customers:
            if customer.enabled:
                logging.info(f"Processing customer {customer.name}...")

                stg_prefix = maincfg.src_container_prefix + customer.source_container
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
                    if customer.file_format.lower() == "csv":
                        data = df_final.to_csv(
                            index=False, encoding='utf-8', sep=";", decimal=".")
                        blob_name = f"tapahtumat_{customer.name}_{ts}.csv"
                    elif customer.file_format.lower() == "json":
                        data = JSONBuilder(customer).build_json(df_final)
                        blob_name = f"tapahtumat_{customer.name}_{ts}.json"
                    else:
                        raise ValueError(
                            f"Invalid file format: {customer.file_format}")

                    dst_stg = StorageHandler(
                        customer.destination_container, verify_existence=True)
                    dst_stg.upload_blob(blob_name, data)

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
