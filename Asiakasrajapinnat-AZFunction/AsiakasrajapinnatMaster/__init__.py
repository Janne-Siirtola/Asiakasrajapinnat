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

log_messages = []


def log(msg: str):
    """Append a log string to our in-memory list of messages."""
    log_messages.append(msg)


def get_timestamp(strftime: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Return the current timestamp in the format 'YYYY-MM-DD_HH%M'
    in Finland timezone.
    """
    finland_tz = pytz.timezone('Europe/Helsinki')
    finland_time = datetime.now(finland_tz)
    return finland_time.strftime(strftime)


def load_main_config(storage: StorageHandler) -> MainConfig:
    """
    Load the JSON configuration from the given path and return a MainConfig object.

    :param config_path: either the file path to config.json or a directory containing config.json.
    :return: a MainConfig instance
    """
    json_data = storage.download_blob("MainConfig.json")
    raw = json.loads(json_data)
    # raw["customer_config_path"] = Path(raw["customer_config_path"])
    return MainConfig(**raw)


def load_customers_from_config(maincfg: MainConfig, storage: StorageHandler) -> List[Customer]:
    customers: List[Customer] = []
    for cfg_file in storage.list_json_blobs(prefix=maincfg.customer_config_path):
        json_data = storage.download_blob(cfg_file)
        data = json.loads(json_data)
        customer = Customer(**data, base_columns=maincfg.base_columns, log_func=log)
        # customer.set_base_columns(maincfg.base_columns)
        customers.append(customer)
    return customers


def main(mytimer: func.TimerRequest) -> None:
    try:
        logging.basicConfig(
            level=logging.INFO,
        )
        log(f"Process started at {get_timestamp()}.")
        start_time = time.perf_counter()

        conf_stg = StorageHandler(container_name="asiakasrajapinnat", log_func=log)
        src_stg = StorageHandler(container_name="vitecpowerbi", log_func=log)
        
        maincfg = load_main_config(conf_stg)
        customers = load_customers_from_config(maincfg, conf_stg)
        log(f"Loaded {len(customers)} customers from config.")
        for customer in customers:
            if customer.enabled:
                log(f"Processing customer {customer.name}...")
                
                dst_stg = StorageHandler(customer.destination_container, log_func=log)

                stg_prefix = maincfg.src_container_prefix + customer.source_container
                if not stg_prefix:
                    log(f"Source container for customer {customer.name} is empty.")
                    continue
                
                df = customer.get_data(src_stg, stg_prefix)
                if df.empty:
                    log(f"No data found for customer {customer.name}.")
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

                    json_data = JSONBuilder(customer).build_json(df_final)
                    
                    ts = get_timestamp(strftime="%Y-%m-%d_%H-%M-%S")
                    blob_name = f"tapahtumat_{customer.name}_{ts}"

                    dst_stg.upload_blob(blob_name, json_data)

                    """ df_final.to_csv(f"output.csv", index=False,
                                    encoding='utf-8', sep=";", decimal=".") """
                    log(
                        f"Processed customer {customer.name} successfully.")
                except ValueError as e:
                    logging.error(
                        f"Error processing customer {customer.name}: {e}")
                    continue
                
        elapsed_time = time.perf_counter() - start_time

        log(f"Process completed successfully at {get_timestamp()}.")
        log(f"Elapsed time: {elapsed_time:.2f} seconds.")

        logging.info("\n".join(log_messages))
    except Exception as e:
        # ----------------------------------------
        # 5. FAILURE: OUTPUT LOG + STACK TRACE
        # ----------------------------------------
        logging.error("Unexpected error occurred: " + traceback.format_exc())

        # Optionally re-raise if you want the Azure Function to register as 'failed'
        raise

