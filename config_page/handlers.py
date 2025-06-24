"""Request handlers for the configuration page."""

import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs

import azure.functions as func
from azure.core.exceptions import AzureError
from azure.storage.blob import ContentSettings
from jinja2 import TemplateError

from asiakasrajapinnat_master.main_config import load_main_config

from .form_parser import parse_form_data
from .storage_utils import conf_stg, get_customers
from .exceptions import ClientError, InvalidInputError
from .utils import (
    flash,
    get_css_blocks,
    get_html_blocks,
    get_js_blocks,
    render_template,
    generate_csrf_token,
    validate_csrf_token,
    parse_cookie,
)


def prepare_template_context(
    method: str = "",
    messages: Optional[List[Dict[str, str]]] = None,
    csrf_token: str = "",
) -> Dict[str, Any]:
    """Collect template data for rendering HTML pages."""
    logging.info("Preparing template context for method '%s'", method)
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
        template_name = "edit_base_columns_form.html"
        css_blocks = get_css_blocks(file_specific_styles=["edit_base_columns.css"])
        js_blocks = get_js_blocks(file_specific_scripts=["edit_base_columns.js"])
    elif method == "home":
        template_name = "index.html"
        css_blocks = get_css_blocks(file_specific_styles=["index.css"])
        js_blocks = get_js_blocks()
    else:
        logging.error("Unknown method '%s' in request", method)
        raise ClientError(f"Unknown method '{method}'")

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
        "csrf_token": csrf_token,
    }


def handle_error(err: Exception) -> func.HttpResponse:
    """Return a generic 500 response and log the stack trace."""
    logging.exception("Unexpected error: %s", err)
    return func.HttpResponse(
        json.dumps({"error": "Internal server error"}),
        status_code=500,
        mimetype="application/json",
    )


def handle_get(req: func.HttpRequest) -> func.HttpResponse:
    """Process a GET request."""
    try:
        logging.info("Processing GET request")
        method = req.params.get("method", "").strip()
        messages: List[Dict[str, str]] = []
        token, cookie_val = generate_csrf_token()
        context = prepare_template_context(
            method=method, messages=messages, csrf_token=token
        )
        context["headers"] = {
            "Set-Cookie": f"csrf_token={cookie_val}; HttpOnly; Path=/"
        }
        logging.info("Returning template %s", context["template_name"])
        return render_template(context)
    except (TemplateError, AzureError) as err:
        return handle_error(err)


def handle_post(req: func.HttpRequest) -> func.HttpResponse:
    """Process a POST request."""
    try:
        logging.info("Processing POST request")
        messages: List[Dict[str, str]] = []
        raw_body = req.get_body().decode("utf-8")
        parsed = parse_qs(raw_body, keep_blank_values=True)
        form_token = parsed.get("csrf_token", [""])[0]
        cookie_header = req.headers.get("Cookie", "")
        cookie_token = parse_cookie(cookie_header).get("csrf_token", "")
        if not validate_csrf_token(form_token, cookie_token):
            logging.warning("Invalid CSRF token")
            flash(messages, "error", "Invalid form submission.")
            token, cookie_val = generate_csrf_token()
            context = prepare_template_context(messages=messages, csrf_token=token)
            context["headers"] = {
                "Set-Cookie": f"csrf_token={cookie_val}; HttpOnly; Path=/"
            }
            context["status_code"] = 400
            return render_template(context)

        try:
            method, result = parse_form_data(raw_body, messages)
            name = result.get("name") if isinstance(result, dict) else ""
            original_name = result.pop("original_name", name) if isinstance(result, dict) else name
        except (InvalidInputError, json.JSONDecodeError, AzureError) as err:
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
        if method == "create_customer" or (method == "edit_customer" and original_name != name):
            json_blob_exists = conf_stg.blob_exists(
                f"customer_config/{name}.json")

        if method in ["create_customer", "edit_customer"]:
            if json_blob_exists and (method == "create_customer" or original_name != name):
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
                if method == "edit_customer" and original_name != name:
                    try:
                        conf_stg.container_client.delete_blob(
                            f"customer_config/{original_name}.json")
                        logging.info(
                            "Renamed customer '%s' to '%s'", original_name, name)
                    except AzureError as e:
                        logging.error(
                            "Failed to delete old config for '%s': %s",
                            original_name,
                            e,
                        )
                        flash(
                            messages,
                            "error",
                            f"Failed to remove old config '{original_name}': {e}",
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
        elif method == "update_enabled":
            for cname, state in result.items():
                try:
                    raw = conf_stg.download_blob(
                        f"customer_config/{cname}.json")
                    cfg = json.loads(raw)
                    cfg["enabled"] = bool(state)
                    conf_stg.upload_blob(
                        blob_name=f"customer_config/{cname}.json",
                        data=json.dumps(
                            cfg, ensure_ascii=False).encode("utf-8"),
                        overwrite=True,
                        content_settings=ContentSettings(
                            content_type="application/json; charset=utf-8"
                        ),
                    )
                except (AzureError, json.JSONDecodeError) as e:
                    logging.error(
                        "Failed to update enabled for '%s': %s", cname, e)
                    flash(messages, "error",
                          f"Failed to update '{cname}': {e}")

        error_occurred = any(f["category"] == "error" for f in messages)
        next_method = method
        if not error_occurred:
            if method == "create_customer":
                flash(messages, "success",
                    f"Asiakas '{name}' luotu onnistuneesti.")
                next_method = "create_customer"
            elif method == "edit_customer":
                flash(messages, "success",
                    f"Asiakas '{name}' päivitetty onnistuneesti.")
                next_method = "edit_customer"
            elif method == "delete_customer":
                flash(messages, "success",
                    f"Asiakas '{result}' poistettu onnistuneesti.")
                next_method = "edit_customer"
            elif method == "edit_base_columns":
                flash(messages, "success", "Perussarakkeet päivitetty.")
                next_method = "edit_base_columns"
            elif method == "update_enabled":
                flash(messages, "success", "Asiakkaiden tilat päivitetty.")
                next_method = "edit_customer"

        token, cookie_val = generate_csrf_token()
        context = prepare_template_context(method=next_method, messages=messages, csrf_token=token)
        context["headers"] = {
            "Set-Cookie": f"csrf_token={cookie_val}; HttpOnly; Path=/"
        }
        logging.info("POST request processed successfully. Returning template %s",
            context["template_name"]
        )
        return render_template(context)
    except (AzureError, TemplateError, ClientError) as err:
        return handle_error(err)
