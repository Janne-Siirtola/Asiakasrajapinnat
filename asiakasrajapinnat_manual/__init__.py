import json
import logging
from typing import Dict, List

import azure.functions as func

from asiakasrajapinnat_master import load_customers_from_config, process_customer
from asiakasrajapinnat_master.customer import Customer, CustomerConfig
from asiakasrajapinnat_master.main_config import load_main_config
from asiakasrajapinnat_master.storage_handler import StorageHandler
from asiakasrajapinnat_master.database_handler import DatabaseHandler


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    cust_names = []
    name = req.params.get('name', '')
    if name:
        cust_names.append(name)
    else:
        logging.error("No customer name provided in the request.")
        return func.HttpResponse(
            "invalid_name",
            status_code=400
        )

    conf_stg = StorageHandler(
        container_name="asiakasrajapinnat", verify_existence=True
    )
    src_stg = StorageHandler(
        container_name="vitecpowerbi", verify_existence=True
    )

    maincfg = load_main_config(conf_stg)
    customers = load_customers_from_config(maincfg.base_columns, conf_stg)

    db = DatabaseHandler(base_columns=maincfg.base_columns, pw_login=True)

    filtered_customers = [
        cust for cust in customers if cust.config.name in cust_names
    ]

    if not filtered_customers:
        return func.HttpResponse(
            f"invalid_name",
            status_code=404
        )

    try:
        for customer in filtered_customers:
            resp = process_customer(customer, src_stg, db)

    except Exception as e:
        logging.error("Error processing customers: %s", e)
        return func.HttpResponse(
            f"error",
            status_code=500
        )

    return func.HttpResponse(
        resp,
        status_code=200
    )
