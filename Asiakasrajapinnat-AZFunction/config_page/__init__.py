"""HTTP endpoint serving the configuration UI and form processing."""

import json
import logging
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs

import azure.functions as func
from azure.core.exceptions import AzureError
from azure.storage.blob import ContentSettings
from jinja2 import Environment, FileSystemLoader, TemplateError, select_autoescape

from asiakasrajapinnat_master.storage_handler import StorageHandler
from asiakasrajapinnat_master.main_config import load_main_config

# version 1.21
src_stg = StorageHandler(container_name="vitecpowerbi")
conf_stg = StorageHandler(container_name="asiakasrajapinnat")

module_dir = os.path.dirname(__file__)
templates_dir = os.path.join(module_dir, "templates")
static_dir = os.path.join(module_dir, "static")
css_dir = os.path.join(static_dir, "css")
js_dir = os.path.join(static_dir, "js")

jinja_env = Environment(
    loader=FileSystemLoader(templates_dir),
    autoescape=select_autoescape(["html", "xml"])
)

flash_messages = []


def flash(category: str = "error", message: str = "") -> None:
    """Add a flash message to the global list.

    Args:
        category (str): The category of the message, e.g. "success", "error". Defaults to "error".
        message (str): The message text to display.

    """
    flash_messages.append({
        "category": category,
        "message": message
    })


def render_template(context: Dict[str, Any]) -> func.HttpResponse:
    """Render a Jinja2 template using values from ``context``."""

    template_name = context.get("template_name", "")
    status_code = context.get("status_code", 200)
    mimetype = context.get("mimetype", "text/html")

    render_args = context.copy()
    messages = render_args.get("messages") or flash_messages
    render_args["messages"] = messages
    render_args.pop("template_name", None)
    render_args.pop("status_code", None)
    render_args.pop("mimetype", None)

    template = jinja_env.get_template(template_name)
    rendered = template.render(**render_args)
    return func.HttpResponse(rendered, status_code=status_code, mimetype=mimetype)


def get_css_blocks(file_specific_styles: Optional[List[str]] = None) -> List[str]:
    """Return a list of CSS snippets for the page."""

    css_blocks = []

    global_styles = ["base.css", "flash.css", "navbar.css"]

    # Add global styles
    for css_file in global_styles:
        css_path = os.path.join(css_dir, css_file)
        if os.path.exists(css_path):
            with open(css_path, 'r', encoding='utf-8') as f:
                css_blocks.append(f.read())

    # Add file-specific styles if provided
    if file_specific_styles:
        for css_file in file_specific_styles:
            css_path = os.path.join(css_dir, css_file)
            if os.path.exists(css_path):
                with open(css_path, 'r', encoding='utf-8') as f:
                    css_blocks.append(f.read())

    return css_blocks


def get_js_blocks(file_specific_scripts: Optional[List[str]] = None) -> List[str]:
    """Return a list of JavaScript snippets for the page."""

    js_blocks = []

    # Add global scripts
    global_scripts = ["navbar.js", "flash.js"]

    for js_file in global_scripts:
        js_path = os.path.join(js_dir, js_file)
        if os.path.exists(js_path):
            with open(js_path, 'r', encoding='utf-8') as f:
                js_blocks.append(f.read())

    # Add file-specific scripts if provided
    if file_specific_scripts:
        for js_file in file_specific_scripts:
            js_path = os.path.join(js_dir, js_file)
            if os.path.exists(js_path):
                with open(js_path, 'r', encoding='utf-8') as f:
                    js_blocks.append(f.read())

    return js_blocks


def get_html_blocks(file_specific_html: Optional[List[str]] = None) -> List[Dict[str, str]]:
    """Return HTML template fragments to include in the page."""

    html_blocks = []

    # Add global HTML blocks
    global_html = ["navbar.html"]

    for html_file in global_html:
        html_path = os.path.join(templates_dir, html_file)
        if os.path.exists(html_path):
            with open(html_path, 'r', encoding='utf-8') as f:
                html_blocks.append({
                    "name": html_file,
                    "content": f.read()
                })

    # Add file-specific HTML blocks if provided
    if file_specific_html:
        for html_file in file_specific_html:
            html_path = os.path.join(templates_dir, html_file)
            if os.path.exists(html_path):
                with open(html_path, 'r', encoding='utf-8') as f:
                    html_blocks.append({
                        "name": html_file,
                        "content": f.read()
                    })

    return html_blocks


def create_containers(src_container: str, dest_container: str):
    """Create source and destination containers if they do not exist."""

    # Create source directory + history directory
    prefix = f"Rajapinta/{src_container}"
    history_dir = prefix + 'history/'

    list_blobs = src_stg.list_blobs(prefix=prefix)
    if not list_blobs:
        try:
            marker = history_dir + '.keep'
            src_stg.upload_blob(marker, b'', overwrite=True)
            src_stg.container_client.delete_blob(marker)
        except AzureError as e:
            # if something goes wrong it’s non‐fatal—just log it
            logging.error(
                "Could not create directory marker %s: %s",
                marker,
                e,
            )
    else:
        src_container = src_container.strip("/")  # Ensure no trailing slash
        flash(
            "error", f"Source container '{src_container}' "
            "already exists. Please choose a different name.")

    # Create destination container
    dst_stg = StorageHandler(container_name=dest_container)

    if dst_stg.container_exists():
        dest_container = dest_container.strip("/")  # Ensure no trailing slash
        flash(
            "error", f"Destination container '{dest_container}' already exists. "
            "Please choose a different name.")
    else:
        try:
            dst_stg.create_container()
            logging.info(
                "Destination container '%s' created.",
                dest_container,
            )
        except AzureError as e:
            flash("error", f"Failed to create destination container: {e}")
            logging.error(
                "Failed to create destination container: %s",
                e,
            )


def get_customers() -> List[str]:
    """Load customer configuration files from storage."""

    customers = []
    try:
        for cfg_file in conf_stg.list_json_blobs("CustomerConfig"):
            try:
                raw = conf_stg.download_blob(cfg_file)
                data = json.loads(raw)
                # Optionally, you can add the blob name or key so you know which is which:
                # e.g. data["_blob_name"] = cfg_file
                customers.append(data)
            except (AzureError, json.JSONDecodeError) as e:
                logging.error(
                    "Failed to parse JSON from blob '%s': %s",
                    cfg_file,
                    e,
                )
                continue
    except AzureError as e:
        logging.error("Failed to list blobs under CustomerConfig/: %s", e)

    return customers


def prepare_template_context(
    method: str = "",
    messages: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Collect template data for rendering HTML pages."""

    if messages is None:
        messages = flash_messages

    template_name = "customer_config_form.html"

    if method == "edit_customer":
        css_blocks = get_css_blocks(
            file_specific_styles=["customer_config.css"])
        js_blocks = get_js_blocks(file_specific_scripts=["customer_config.js"])
    elif method == "create_customer":
        css_blocks = get_css_blocks(
            file_specific_styles=["customer_config.css"])
        js_blocks = get_js_blocks(file_specific_scripts=["customer_config.js"])
    elif method == "edit_basecols":
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
        "base_columns": main_config.base_columns
    }


def _parse_base_columns(parsed: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
    """Extract base column configuration from parsed form data."""

    keys = parsed.get("key", [])
    names = parsed.get("name", [])
    dtypes = parsed.get("dtype", [])
    decimals = parsed.get("decimals", [])

    basecols: Dict[str, Dict[str, Any]] = {}
    for k, n, dt, dec in zip(keys, names, dtypes, decimals):
        k = k.strip()
        if not k:
            continue
        col = {"name": n.strip(), "dtype": dt.strip()}
        if dt.strip() == "float" and dec.strip():
            try:
                col["decimals"] = int(dec)
            except ValueError:
                flash(
                    "error",
                    f"Invalid decimal value for column '{k}': {dec.strip()}",
                )
        basecols[k] = col

    return basecols


def _parse_konserni_list(raw_value: str) -> List[int]:
    """Parse a comma separated list of konserni ids."""

    konserni_list: List[int] = []
    for part in filter(None, [p.strip() for p in raw_value.split(",")]):
        try:
            konserni_list.append(int(part))
        except ValueError:
            logging.warning("Ignoring non-numeric konserni token: '%s'", part)
            flash(
                "error",
                f"Invalid konserni value: '{part}'. Please enter numeric values only.",
            )

    return konserni_list


def _parse_extra_columns(parsed: Dict[str, List[str]]) -> Dict[str, Dict[str, str]]:
    """Extract extra column configuration from parsed form data."""

    extra_keys = parsed.get("extra_key", [])
    extra_names = parsed.get("extra_name", [])
    extra_dtypes = parsed.get("extra_dtype", [])

    extra_columns: Dict[str, Dict[str, str]] = {}
    for key, disp, dt in zip(extra_keys, extra_names, extra_dtypes):
        key = key.strip()
        if key:
            extra_columns[key] = {"name": disp.strip(), "dtype": dt.strip()}

    return extra_columns


def parse_form_data(body: str) -> Tuple[str, Any]:
    """Parse POSTed form data and return method and configuration."""

    parsed = parse_qs(body, keep_blank_values=True)

    method = parsed.get("method", [""])[0].strip().lower()
    if method == "edit_basecols":
        basecols = _parse_base_columns(parsed)
        return method, basecols

    if method not in ["create_customer", "edit_customer"]:
        raise ValueError("Invalid method")

    # Determine enabled state
    if method == "create_customer":
        enabled = True
    else:
        enabled = parsed.get("enabled", [""])[0].strip().lower() == "true"

    name = parsed.get("name", [""])[0].strip().lower()

    konserni_raw = parsed.get("konserni", [""])[0].strip()
    konserni_list = _parse_konserni_list(konserni_raw)

    src_container = parsed.get("src_container", [""])[0].strip().lower() + "/"
    dest_container = parsed.get("dest_container", [""])[
        0].strip().lower() + "/"
    file_format = parsed.get("file_format", [""])[0].strip().lower()
    file_encoding = parsed.get("file_encoding", [""])[0].strip().lower()

    extra_columns = _parse_extra_columns(parsed)

    exclude_list = parsed.get("exclude_columns", [])

    if method == "create_customer":
        check_str = parsed.get("create_containers_check", [""])[
            0].strip().lower()
        if check_str == "true":
            create_containers(src_container, dest_container)

    result = {
        "enabled": enabled,
        "name": name,
        "konserni": konserni_list,
        "source_container": src_container,
        "destination_container": dest_container,
        "file_format": file_format,
        "file_encoding": file_encoding,
        "extra_columns": extra_columns,
        "exclude_columns": exclude_list,
    }

    return method, result


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function entry point for serving and processing the form."""

    logging.info("ServeConfig function processed a request.")

    flash_messages.clear()  # Clear previous flash messages

    # 1) If GET → just return the HTML form
    if req.method == "GET":
        try:
            method = req.params.get("method", "").strip()

            context = prepare_template_context(method=method, messages=[])
            return render_template(context)
        except (TemplateError, AzureError) as e:
            logging.error("Error rendering template: %s", e)
            return func.HttpResponse(
                json.dumps({"error_in_get": str(traceback.format_exc())}),
                status_code=500,
                mimetype="application/json"
            )

    # 2) If POST → parse everything and build the JSON
    elif req.method == "POST":
        try:
            try:
                raw_body = req.get_body().decode("utf-8")
                method, result = parse_form_data(raw_body)
                name = result["name"] if isinstance(
                    result, dict) and "name" in result else ""
            except (ValueError, json.JSONDecodeError, AzureError) as e:
                logging.error("Failed to parse POST body: %s", e)
                return func.HttpResponse(
                    json.dumps({"error": str(e)}),
                    status_code=400,
                    mimetype="application/json"
                )

            # Upload the config JSON to Azure Blob Storage
            if method == "edit_basecols":
                new_cfg = {
                    "base_columns": result
                }
                conf_stg.upload_blob(
                    "MainConfig.json",
                    json.dumps(new_cfg, ensure_ascii=False).encode("utf-8"),
                    overwrite=True,
                    content_settings=ContentSettings(
                        content_type="application/json; charset=utf-8")
                )
            json_blob_exists = False
            if method == "create_customer":
                json_blob_exists = conf_stg.list_json_blobs(
                    prefix=f"CustomerConfig/{name}")
            if method in ["create_customer", "edit_customer"]:
                # If the blob already exists, raise an error (skip check for edit)
                if json_blob_exists and method == "create_customer":
                    logging.error(
                        "Configuration for customer '%s' already exists.",
                        name,
                    )
                    flash(
                        "error", f"Configuration for customer '{name}' already exists. "
                        "Please choose a different name.")
                else:  # If it does not exist, upload the new configuration
                    logging.info(
                        "Uploading configuration for customer '%s'",
                        name,
                    )
                    conf_stg.upload_blob(
                        blob_name=f"CustomerConfig/{name}.json",
                        data=json.dumps(
                            result, ensure_ascii=False).encode("utf-8"),
                        overwrite=True,
                        content_settings=ContentSettings(
                            content_type="application/json; charset=utf-8")
                    )

            # 4) Add a success message if no errors occurred
            error_occurred = any(
                f["category"] == "error" for f in flash_messages)

            if method == "create_customer" and not error_occurred:
                flash("success", f"Customer '{name}' created successfully.")
            elif method == "edit_customer" and not error_occurred:
                flash("success", f"Customer '{name}' updated successfully.")
            elif method == "edit_basecols" and not error_occurred:
                flash("success", "Base columns updated successfully.")

            # DEBUG: Return the result as JSON for debugging purposes
            # return func.HttpResponse(
            #     json.dumps(result, ensure_ascii=False),
            #     status_code=200,
            #     mimetype="application/json"
            # )

            # Redirect to the index page after processing
            context = prepare_template_context()
            return render_template(context)
        except (AzureError, TemplateError, ValueError) as e:
            logging.error("Error processing POST request: %s", e)
            return func.HttpResponse(
                json.dumps({"error_in_post": str(traceback.format_exc())}),
                status_code=500,
                mimetype="application/json"
            )
    # 3) Any other HTTP verb → 405
    else:
        return func.HttpResponse(
            "Method Not Allowed",
            status_code=405,
            mimetype="text/plain"
        )
