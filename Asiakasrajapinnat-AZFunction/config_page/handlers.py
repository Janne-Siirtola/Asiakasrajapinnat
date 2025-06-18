"""Request handlers for the configuration page."""

import json
import logging
import traceback
from typing import Any, Dict, List, Optional

import azure.functions as func
from azure.core.exceptions import AzureError
from azure.storage.blob import ContentSettings
from jinja2 import TemplateError

from asiakasrajapinnat_master.main_config import load_main_config

from .form_parser import parse_form_data
from .storage_utils import conf_stg, get_customers
from .utils import (
    flash,
    get_css_blocks,
    get_html_blocks,
    get_js_blocks,
    render_template,
)


def prepare_template_context(
    method: str = "",
    messages: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Collect template data for rendering HTML pages."""
    if messages is None:
        messages = []

    template_name = "customer_config_form.html"

    if method == "edit_customer":
        css_blocks = get_css_blocks(
            file_specific_styles=["customer_config.css"])
        js_blocks = get_js_blocks(file_specific_scripts=["customer_config.js"])
    elif method == "create_customer":
        css_blocks = get_css_blocks(
            file_specific_styles=["customer_config.css"])
        js_blocks = get_js_blocks(file_specific_scripts=["customer_config.js"])
    elif method == "edit_base_columns":
        template_name = "basecols_form.html"
        css_blocks = get_css_blocks(file_specific_styles=["basecols_form.css"])
        js_blocks = get_js_blocks(file_specific_scripts=["basecols_form.js"])
    else:
        template_name = "index.html"
        css_blocks = get_css_blocks(file_specific_styles=["index.css"])
        js_blocks = get_js_blocks()

    html_blocks = get_html_blocks()

    customers = get_customers()
    main_config = load_main_config(conf_stg)

    return {
        "template_name": template_name,
        "method": method,
        "css_blocks": css_blocks,
        "js_blocks": js_blocks,
        "html_blocks": html_blocks,
        "customers": customers,
        "messages": messages,
        "base_columns": main_config.base_columns,
    }


def handle_error(err: Exception) -> func.HttpResponse:
    """Return a 500 ``HttpResponse`` with the error message."""
    logging.error("Unexpected error: %s", err)
    return func.HttpResponse(
        json.dumps({"error": str(traceback.format_exc())}),
        status_code=500,
        mimetype="application/json",
    )


def handle_get(req: func.HttpRequest) -> func.HttpResponse:
    """Process a GET request."""
    try:
        method = req.params.get("method", "").strip()
        messages: List[Dict[str, str]] = []
        context = prepare_template_context(method=method, messages=messages)
        return render_template(context)
    except (TemplateError, AzureError) as err:
        return handle_error(err)


def handle_post(req: func.HttpRequest) -> func.HttpResponse:
    """Process a POST request."""
    try:
        messages: List[Dict[str, str]] = []
        try:
            raw_body = req.get_body().decode("utf-8")
            method, result = parse_form_data(raw_body, messages)
            name = result["name"] if isinstance(
                result, dict) and "name" in result else ""
        except (ValueError, json.JSONDecodeError, AzureError) as err:
            logging.error("Failed to parse POST body: %s", err)
            return func.HttpResponse(
                json.dumps({"error": str(err)}),
                status_code=400,
                mimetype="application/json",
            )

        if method == "edit_base_columns":
            new_cfg = {"base_columns": result}
            conf_stg.upload_blob(
                "main_config.json",
                json.dumps(new_cfg, ensure_ascii=False).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(
                    content_type="application/json; charset=utf-8"
                ),
            )

        json_blob_exists = False
        if method == "create_customer":
            json_blob_exists = conf_stg.list_json_blobs(
                prefix=f"customer_config/{name}")

        if method in ["create_customer", "edit_customer"]:
            if json_blob_exists and method == "create_customer":
                logging.error(
                    "Configuration for customer '%s' already exists.", name)
                flash(
                    messages,
                    "error",
                    f"Configuration for customer '{name}' already exists. "
                    "Please choose a different name.",
                )
            else:
                logging.info("Uploading configuration for customer '%s'", name)
                conf_stg.upload_blob(
                    blob_name=f"customer_config/{name}.json",
                    data=json.dumps(
                        result, ensure_ascii=False).encode("utf-8"),
                    overwrite=True,
                    content_settings=ContentSettings(
                        content_type="application/json; charset=utf-8"
                    ),
                )
        elif method == "delete_customer":
            try:
                conf_stg.container_client.delete_blob(
                    f"customer_config/{result}.json")
                logging.info("Deleted configuration for customer '%s'", result)
            except AzureError as e:
                logging.error("Failed to delete customer '%s': %s", result, e)
                flash(messages, "error",
                      f"Failed to delete customer '{result}': {e}")

        error_occurred = any(f["category"] == "error" for f in messages)

        if method == "create_customer" and not error_occurred:
            flash(messages, "success",
                  f"Customer '{name}' created successfully.")
        elif method == "edit_customer" and not error_occurred:
            flash(messages, "success",
                  f"Customer '{name}' updated successfully.")
        elif method == "delete_customer" and not error_occurred:
            flash(messages, "success",
                  f"Customer '{result}' deleted successfully.")
        elif method == "edit_base_columns" and not error_occurred:
            flash(messages, "success", "Base columns updated successfully.")

        context = prepare_template_context(messages=messages)
        return render_template(context)
    except (AzureError, TemplateError, ValueError) as err:
        return handle_error(err)
