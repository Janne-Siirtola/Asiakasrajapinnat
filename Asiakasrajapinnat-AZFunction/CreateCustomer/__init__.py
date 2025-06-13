import logging
import traceback
import azure.functions as func
from typing import Dict, List, Optional
import os
import json
from urllib.parse import parse_qs

from AsiakasrajapinnatMaster.StorageHandler import StorageHandler
from jinja2 import Environment, FileSystemLoader, select_autoescape

# version 1.21

"""
TODO:
- Flash messages for errors and success
"""

module_dir = os.path.dirname(__file__)
templates_dir = "templates"
static_dir = "static"
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
    status_code: int = 200,
    mimetype: str = "text/html",
    css_blocks: List[str] = None,
) -> func.HttpResponse:
    """
    :param template_name: e.g. "index.html"
    :param context:       passed straight to template.render()
    """
    template = jinja_env.get_template(template_name)
    rendered = template.render(messages=flash_messages, css_blocks=css_blocks)
    return func.HttpResponse(rendered, status_code=status_code, mimetype=mimetype)

def get_css_blocks(file_specific_styles: Optional[List[str]]) -> List[str]:
    css_blocks = []
    css_files = os.listdir(css_dir)
    
    global_styles = ["base.css", "flash.css"]

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


def create_containers(src_container: str, dest_container: str):
    src_stg = StorageHandler(
        container_name="vitecpowerbi")

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
        flash("error", f"Source container '{src_container}' already exists. "
                     "Please choose a different name.")
        

    # Create destination container
    dst_stg = StorageHandler(container_name=dest_container)
    
    if dst_stg.container_exists():
        flash("error", f"Destination container '{dest_container}' already exists. "
                     "Please choose a different name.")
    else:
        try:
            dst_stg.create_container()
            logging.info(f"Destination container '{dest_container}' created.")
        except Exception as e:
            flash("error", f"Failed to create destination container: {e}")
            logging.error(f"Failed to create destination container: {e}")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ServeConfig function processed a request.")

    # 1) If GET → just return the HTML form
    if req.method == "GET":
        try:
            method = req.params.get("method", "").strip()
            
            if method == "edit":
                template = "EditCustomer.html"
            elif method == "create":
                template = "CreateCustomer.html"
            else:
                return func.HttpResponse(
                    "Method not specified or invalid. Use 'edit' or 'create'.",
                    status_code=400,
                    mimetype="text/plain"
                )

            # Get CSS blocks to inject into the template
            css_blocks = get_css_blocks(file_specific_styles=[template.replace(".html", ".css")])
            return render_template(
                template_name=template,
                css_blocks=css_blocks
            )
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

            # 2.a) “enabled” is always True
            enabled = True

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

            # 2.g) create_containers_check → boolean (true/false)
            check_str = parsed.get("create_containers_check", [""])[0].strip()
            if check_str == "true":
                create_containers(src_container, dest_container)
                check = True
            else:
                check = False

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
                "create_containers_check": check
            }

            # Upload the config JSON to Azure Blob Storage
            conf_stg = StorageHandler(container_name="asiakasrajapinnat")

            json_blob_exists = conf_stg.list_json_blobs(prefix=f"CustomerConfig/{name}")
            if json_blob_exists: # If the blob already exists, raise an error
                logging.error(f"Configuration for customer '{name}' already exists.")
                flash("error", f"Configuration for customer '{name}' already exists. "
                     "Please choose a different name or edit the existing configuration.")
            else: # If it does not exist, upload the new configuration
                logging.info(f"Uploading configuration for customer '{name}'")
                conf_stg.upload_blob(
                    blob_name=f"CustomerConfig/{name}.json",
                    data=json.dumps(result, ensure_ascii=False).encode("utf-8"),
                    overwrite=True
                )
            
            # 4) Add a success message
            flash("success", f"Customer '{name}' created successfully.")

            return render_template(
                template_name="index.html"
            )
            # 4) Return it as application/json
            return func.HttpResponse(
                json.dumps(result, ensure_ascii=False),
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
