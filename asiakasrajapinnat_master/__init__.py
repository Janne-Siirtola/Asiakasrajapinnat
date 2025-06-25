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

from .customer import Customer, CustomerConfig
from .data_builder import DataBuilder
from .data_editor import DataEditor
from .main_config import load_main_config
from .storage_handler import StorageHandler
from .esrs_data_parser import EsrsDataParser
from .database_handler import DatabaseHandler


# Silence the Blob SDK’s HTTP logs
logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING)


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
    for cfg_file in storage.list_json_blobs(prefix="customer_config/"):
        json_data = storage.download_blob(cfg_file)
        data = json.loads(json_data)
        cfg = CustomerConfig(base_columns=base_columns, **data)
        customer = Customer(cfg)
        customers.append(customer)
    return customers


def process_customer(
    customer: Customer, src_stg: StorageHandler, db: DatabaseHandler
) -> str | None:
    """Process a single customer and upload the resulting file."""
    if not customer.config.enabled:
        logging.info(
            "Skipping customer %s as it is not enabled.", customer.config.name
        )
        return "Customer is not enabled."

    logging.info("Processing customer %s...", customer.config.name)

    stg_prefix = "Rajapinta/" + customer.config.source_container

    df = customer.get_data(src_stg, stg_prefix)
    if df.empty:
        logging.info("No data found for customer %s.", customer.config.name)
        return "No data in source container."

    editor = DataEditor(df=df, customer=customer)
    df_edited = (
        editor.delete_row(0)
        .validate_concern_number()
        .drop_unmapped_columns()
        .reorder_columns()
        .rename_and_cast_datatypes()
        .format_date_and_time()
        .normalize_null_values()
        .clean_tapahtuma_id()
        .validate_final_df()
        .df
    )

    # Upsert the edited DataFrame to the database
    # And fetch all rows for ESRS parsing
    db.upsert_rows(customer.config.name, df_edited)
    df_fetchall = db.fetch_dataframe(customer.config.name)
    esrs_parser = EsrsDataParser(df_fetchall)
    esrs_json = esrs_parser.parse()

    # Handle excluded base columns after upserting so
    # all needed columns are present in the database.
    df_final = editor.drop_excluded_columns(df_edited)

    ts = get_timestamp(strftime="%Y-%m-%d_%H%M")

    builder = DataBuilder(customer)
    blob_name_base = f"tapahtumat_{ts}"
    if customer.config.file_format.lower() == "csv":
        data = builder.build_csv(
            df_final, encoding=customer.config.file_encoding)
        blob_name = blob_name_base + ".csv"
        content_settings = ContentSettings(
            content_type=f"text/csv; charset={customer.config.file_encoding}"
        )
    elif customer.config.file_format.lower() == "json":
        data = builder.build_json(df_final)
        blob_name = blob_name_base + ".json"
        content_settings = ContentSettings(
            content_type=f"application/octet-stream; charset={customer.config.file_encoding}"
        )
    else:
        raise ValueError(f"Invalid file format: {customer.config.file_format}")

    dst_stg = StorageHandler(
        customer.config.destination_container, verify_existence=True)
    dst_stg.upload_blob(blob_name, data, content_settings=content_settings)

    esrs_blob = f"esrs_report.json"
    esrs_bytes = json.dumps(esrs_json, ensure_ascii=False).encode("utf-8")
    dst_stg.upload_blob(
        esrs_blob,
        esrs_bytes,
        content_settings=ContentSettings(
            content_type="application/json; charset=utf-8"
        ),
    )
    
    # Finally if nothing went wrong, move the src file to history
    src_stg.move_file_to_dir(
        source_blob_name=customer.file_in_process,
        target_dir=stg_prefix + "history/",
        overwrite=True
    )

    logging.info("Processed customer %s successfully.", customer.config.name)
    return "success"

def reprocess_customers(
    customers: List[Customer], src_stg: StorageHandler, db: DatabaseHandler
) -> List[Customer] | None:
    """Reprocess failed customers."""
    failed_customers = []
    
    for customer in customers:
        try:
            process_customer(customer, src_stg, db)
        except Exception as e:
            logging.exception("Error reprocessing customers: %s", e)
            failed_customers.append(customer)

    return failed_customers

def main(mytimer: func.TimerRequest) -> None:
    """Entry point for the timer triggered function."""
    try:
        logging.info("Process started at %s.", get_timestamp())
        start_time = time.perf_counter()

        conf_stg = StorageHandler(
            container_name="asiakasrajapinnat", verify_existence=True
        )
        src_stg = StorageHandler(
            container_name="vitecpowerbi", verify_existence=True
        )

        maincfg = load_main_config(conf_stg)

        customers = load_customers_from_config(maincfg.base_columns, conf_stg)
        logging.info("Loaded %d customers from config.", len(customers))

        db = DatabaseHandler(base_columns=maincfg.base_columns)

        failed_customers = []
        for customer in customers:
            try:
                process_customer(customer, src_stg, db)
            except Exception as err:
                logging.exception(
                    "Error processing customer %s: %s",
                    customer.config.name,
                    err,
                )
                failed_customers.append(customer)
                continue
        
        retry_count = 2
        if failed_customers:
            logging.info("Retrying failed customers...")
            for _ in range(retry_count):
                logging.info("Retry attempt %d", _ + 1)
                failed_customers = reprocess_customers(failed_customers, src_stg, db)
                if not failed_customers:
                    break

        elapsed_time = time.perf_counter() - start_time

        logging.info("Process completed successfully at %s.", get_timestamp())
        logging.info("Elapsed time: %.2f seconds.", elapsed_time)
    except Exception as e:
        # ----------------------------------------
        # 5. FAILURE: OUTPUT LOG + STACK TRACE
        # ----------------------------------------
        logging.exception("Unexpected error occurred: %s", e)

        # Re-raise the exception to ensure the function is marked as failed
        raise
