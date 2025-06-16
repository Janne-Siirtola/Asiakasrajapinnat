import logging
import traceback
import azure.functions as func
from typing import Dict, List, Optional
import os
import json
from urllib.parse import parse_qs

from AsiakasrajapinnatMaster.StorageHandler import StorageHandler
from AsiakasrajapinnatMaster.MainConfig import MainConfig
from jinja2 import Environment, FileSystemLoader, select_autoescape

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

flash_messages = []  # List[Dict[str, str]], List of flash messages to be displayed


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


def render_template(
    template_name: str,
    method: str,
    status_code: int = 200,
    mimetype: str = "text/html",
    css_blocks: List[str] = None,
    js_blocks: List[str] = None,
    html_blocks: List[Dict[str, str]] = None,
    customers: List = None,
    messages: List[Dict[str, str]] = None,
    base_columns: Dict[str, Dict[str, str]] = None
) -> func.HttpResponse:
    """
    :param template_name: e.g. "index.html"
    :param context:       passed straight to template.render()
    """
    if not messages:
        messages = flash_messages

    template = jinja_env.get_template(template_name)
    rendered = template.render(messages=messages, method=method, css_blocks=css_blocks,
                               js_blocks=js_blocks, html_blocks=html_blocks, customers=customers,
                               base_columns=base_columns)
    return func.HttpResponse(rendered, status_code=status_code, mimetype=mimetype)


def get_css_blocks(file_specific_styles: Optional[List[str]] = None) -> List[str]:
    css_blocks = []

    global_styles = ["base.css", "flash.css", "navbar.css"]

    # Add global styles
    for css_file in global_styles:
        css_path = os.path.join(css_dir, css_file)
        if os.path.exists(css_path):
            with open(css_path, 'r') as f:
                css_blocks.append(f.read())

    # Add file-specific styles if provided
    if file_specific_styles:
        for css_file in file_specific_styles:
            css_path = os.path.join(css_dir, css_file)
            if os.path.exists(css_path):
                with open(css_path, 'r') as f:
                    css_blocks.append(f.read())

    return css_blocks


def get_js_blocks(file_specific_scripts: Optional[List[str]] = None) -> List[str]:
    js_blocks = []

    # Add global scripts
    global_scripts = ["navbar.js", "flash.js"]

    for js_file in global_scripts:
        js_path = os.path.join(js_dir, js_file)
        if os.path.exists(js_path):
            with open(js_path, 'r') as f:
                js_blocks.append(f.read())
                
    # Add file-specific scripts if provided
    if file_specific_scripts:
        for js_file in file_specific_scripts:
            js_path = os.path.join(js_dir, js_file)
            if os.path.exists(js_path):
                with open(js_path, 'r') as f:
                    js_blocks.append(f.read())


    return js_blocks


def get_html_blocks(file_specific_html: Optional[List[str]] = None) -> List[Dict[str, str]]:
    html_blocks = []

    # Add global HTML blocks
    global_html = ["navbar.html"]

    for html_file in global_html:
        html_path = os.path.join(templates_dir, html_file)
        if os.path.exists(html_path):
            with open(html_path, 'r') as f:
                html_blocks.append({
                    "name": html_file,
                    "content": f.read()
                })

    # Add file-specific HTML blocks if provided
    if file_specific_html:
        for html_file in file_specific_html:
            html_path = os.path.join(templates_dir, html_file)
            if os.path.exists(html_path):
                with open(html_path, 'r') as f:
                    html_blocks.append({
                        "name": html_file,
                        "content": f.read()
                    })

    return html_blocks


def create_containers(src_container: str, dest_container: str):

    # Create source directory + history directory
    prefix = f"Rajapinta/{src_container}"
    history_dir = prefix + 'history/'

    list_blobs = src_stg.list_blobs(prefix=prefix)
    if not list_blobs:
        try:
            marker = history_dir + '.keep'
            src_stg.upload_blob(marker, b'', overwrite=True)
            src_stg.container_client.delete_blob(marker)
        except Exception as e:
            # if something goes wrong it’s non‐fatal—just log it
            logging.error(f"Could not create directory marker {marker}: {e}")
    else:
        src_container = src_container.strip("/")  # Ensure no trailing slash
        flash(
            "error", f"Source container '{src_container}' already exists. Please choose a different name.")

    # Create destination container
    dst_stg = StorageHandler(container_name=dest_container)

    if dst_stg.container_exists():
        dest_container = dest_container.strip("/")  # Ensure no trailing slash
        flash(
            "error", f"Destination container '{dest_container}' already exists. Please choose a different name.")
    else:
        try:
            dst_stg.create_container()
            logging.info(f"Destination container '{dest_container}' created.")
        except Exception as e:
            flash("error", f"Failed to create destination container: {e}")
            logging.error(f"Failed to create destination container: {e}")


def get_customers() -> List[str]:

    customers = []
    try:
        for cfg_file in conf_stg.list_json_blobs("CustomerConfig"):
            try:
                raw = conf_stg.download_blob(cfg_file)
                data = json.loads(raw)
                # Optionally, you can add the blob name or key so you know which is which:
                # e.g. data["_blob_name"] = cfg_file
                customers.append(data)
            except Exception as e:
                logging.error(
                    f"Failed to parse JSON from blob '{cfg_file}': {e}")
                continue
    except Exception as e:
        logging.error(f"Failed to list blobs under CustomerConfig/: {e}")

    return customers


def prepare_template_context(method: str = "", messages: List[Dict[str, str]] = flash_messages) -> Dict[str, any]:
    template_name = "config_form.html"

    if method == "edit":
        css_blocks = get_css_blocks(file_specific_styles=["config_form.css"])
        js_blocks = get_js_blocks(file_specific_scripts=["config_form.js", "editCustomer.js"])
    elif method == "create":
        css_blocks = get_css_blocks(
            file_specific_styles=["config_form.css"])
        js_blocks = get_js_blocks(file_specific_scripts=["config_form.js", "createCustomer.js"])
    else:
        template_name = "index.html"
        css_blocks = get_css_blocks(file_specific_styles=["index.css"])
        js_blocks = get_js_blocks()

    html_blocks = get_html_blocks()

    customers = get_customers()
    
    main_config = MainConfig(conf_stg)

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


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ServeConfig function processed a request.")

    flash_messages.clear()  # Clear previous flash messages

    # 1) If GET → just return the HTML form
    if req.method == "GET":
        try:
            method = req.params.get("method", "").strip()

            context = prepare_template_context(method=method, messages=[])
            return render_template(**context)
        except Exception as e:
            logging.error(f"Error rendering template: {e}")
            return func.HttpResponse(
                json.dumps({"error_in_get": str(traceback.format_exc())}),
                status_code=500,
                mimetype="application/json"
            )

    # 2) If POST → parse everything and build the JSON
    elif req.method == "POST":
        try:
            try:
                # Azure Functions HttpRequest gives us raw body bytes:
                raw_body = req.get_body().decode("utf-8")
                # Use parse_qs so that array‐style fields (e.g. extra_key[]) become Python lists
                parsed = parse_qs(raw_body)
            except Exception as e:
                logging.error(f"Failed to parse POST body: {e}")
                return func.HttpResponse(
                    json.dumps({"error": "Could not parse form data"}),
                    status_code=400,
                    mimetype="application/json"
                )

            method = parsed.get("method", [""])[0].strip().lower()
            if method not in ["create", "edit"]:
                logging.error(f"Invalid method: {method}")
                return func.HttpResponse(
                    json.dumps({"error": "Invalid method"}),
                    status_code=400,
                    mimetype="application/json"
                )

            # 2.a) “enabled” is True or False
            # If method is "create", we assume enabled is True by default.
            if method == "create":
                enabled = True
            else:
                enabled = parsed.get("enabled", [""])[0].strip().lower()
                # Convert to boolean
                enabled = enabled.lower() == "true"

            # 2.b) name (string)
            name = parsed.get("name", [""])[0].strip().lower()

            # 2.c) konserni → expect comma-separated numbers
            konserni_raw = parsed.get("konserni", [""])[0].strip()
            konserni_list = []
            if konserni_raw:
                # Split on commas, strip whitespace, convert to int if possible
                for part in konserni_raw.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    try:
                        konserni_list.append(int(part))
                    except ValueError:
                        # If it fails to convert, you can decide to (a) ignore it or (b) store as string.
                        # Here, we’ll ignore non-numeric tokens.
                        logging.warning(
                            f"Ignoring non-numeric konserni token: '{part}'")
                        continue

            # 2.d) source_container & destination_container & file_format
            src_container = parsed.get("src_container", [""])[
                0].strip().lower() + "/"
            dest_container = parsed.get("dest_container", [""])[
                0].strip().lower() + "/"
            file_format = parsed.get("file_format", [""])[0].strip().lower()

            # 2.e) extra_columns → arrays extra_key[], extra_name[], extra_dtype[]
            extra_keys = parsed.get("extra_key", [])
            extra_names = parsed.get("extra_name", [])
            extra_dtypes = parsed.get("extra_dtype", [])

            extra_columns = {}
            # Zip them together. If lengths differ, zip stops at the shortest.
            for key, disp, dt in zip(extra_keys, extra_names, extra_dtypes):
                key = key.strip()
                disp = disp.strip()
                dt = dt.strip()
                # Only add if key is non-empty
                if key:
                    extra_columns[key] = {
                        "name": disp,
                        "dtype": dt
                    }

            # 2.f) exclude_columns → comma-separated list of keys
            exclude_raw = parsed.get("exclude_columns", [""])[0].strip()
            exclude_list = []
            if exclude_raw:
                for part in exclude_raw.split(","):
                    part = part.strip()
                    if part:
                        exclude_list.append(part)

            exclude_list = parsed.get("exclude_columns", [""])
            if method == "create":
                # 2.g) create_containers_check → boolean (true/false)
                check_str = parsed.get("create_containers_check", [""])[
                    0].strip()
                if check_str == "true":
                    create_containers(src_container, dest_container)

            # 3) Build the final JSON structure
            result = {
                "enabled": enabled,
                "name": name,
                "konserni": konserni_list,
                "source_container": src_container,
                "destination_container": dest_container,
                "file_format": file_format,
                "extra_columns": extra_columns,
                "exclude_columns": exclude_list,
            }
            debugresult = {
                "enabled": enabled,
                "name": name,
                "konserni": konserni_list,
                "source_container": src_container,
                "destination_container": dest_container,
                "file_format": file_format,
                "extra_columns": extra_columns,
                "exclude_columns": exclude_list,
            }

            # Upload the config JSON to Azure Blob Storage
            if method == "create":
                json_blob_exists = conf_stg.list_json_blobs(
                    prefix=f"CustomerConfig/{name}")
            else:
                json_blob_exists = False  # Skip check for edit, as we assume the blob exists if editing
            # If the blob already exists, raise an error (skip check for edit)
            if json_blob_exists and method == "create":
                logging.error(
                    f"Configuration for customer '{name}' already exists.")
                flash(
                    "error", f"Configuration for customer '{name}' already exists. Please choose a different name.")
            else:  # If it does not exist, upload the new configuration
                logging.info(f"Uploading configuration for customer '{name}'")
                conf_stg.upload_blob(
                    blob_name=f"CustomerConfig/{name}.json",
                    data=json.dumps(
                        result, ensure_ascii=False).encode("utf-8"),
                    overwrite=True
                )

            # 4) Add a success message if no errors occurred
            has_error = any(f["category"] == "error" for f in flash_messages)

            if method == "create" and not has_error:
                flash("success", f"Customer '{name}' created successfully.")
            elif method == "edit" and not has_error:
                flash("success", f"Customer '{name}' updated successfully.")

            # Redirect to the index page after processing
            context = prepare_template_context()
            return render_template(**context)
            # 4) Return it as application/json
            return func.HttpResponse(
                json.dumps(debugresult, ensure_ascii=False),
                status_code=200,
                mimetype="application/json"
            )
        except Exception as e:
            logging.error(f"Error processing POST request: {e}")
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
