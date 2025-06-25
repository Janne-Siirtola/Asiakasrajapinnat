"""Manual trigger for processing selected customers."""

import json
import logging
from typing import List

import azure.functions as func

from asiakasrajapinnat_master import load_customers_from_config, process_customer
from asiakasrajapinnat_master.main_config import load_main_config
from asiakasrajapinnat_master.storage_handler import StorageHandler
from asiakasrajapinnat_master.database_handler import DatabaseHandler


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Handle GET request for manually running customer processing."""
    logging.info("asiakasrajapinnat_manual triggered")

    names_param = req.params.get("names", "")
    names = [n.strip().lower() for n in names_param.split(",") if n.strip()]

    if not names:
        logging.error("No customer names provided in the request.")
        return func.HttpResponse("invalid_name", status_code=400)

    conf_stg = StorageHandler(
        container_name="asiakasrajapinnat", verify_existence=True
    )
    src_stg = StorageHandler(
        container_name="vitecpowerbi", verify_existence=True
    )

    maincfg = load_main_config(conf_stg)
    customers = load_customers_from_config(maincfg.base_columns, conf_stg)

    db = DatabaseHandler(base_columns=maincfg.base_columns, pw_login=False)

    filtered_customers = [c for c in customers if c.config.name in names]

    if not filtered_customers:
        logging.warning("No matching customers found: %s", names)
        return func.HttpResponse("invalid_name", status_code=400)

    responses = []
    i = 1
    try:
        for customer in filtered_customers:
            resp = process_customer(customer, src_stg, db)
            responses.append({
                "run": i,
                "customer": customer.config.name,
                "response": resp
            })
            i += 1

    except Exception as exc:
        logging.exception("Error processing customers: %s", exc)
        responses.append({
            "run": i,
            "customer": customer.config.name,
            "response": "Ajo epäonnistui"
        })

    return func.HttpResponse(json.dumps(responses), status_code=200)
