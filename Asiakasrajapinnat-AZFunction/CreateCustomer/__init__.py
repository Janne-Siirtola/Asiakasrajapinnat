import logging
import azure.functions as func
import os
import json
from urllib.parse import parse_qs

from AsiakasrajapinnatMaster.StorageHandler import StorageHandler

# version 1.9


def get_html_content() -> str:
    """
    Load the HTML form from the templates directory.

    :return: HTML content as a string
    """
    module_dir = os.path.dirname(__file__)
    html_path = os.path.join(module_dir, "templates", "index.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logging.error(f"Failed to load HTML form: {e}")
        raise


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ServeConfig function processed a request.")

    # 1) If GET → just return the HTML form
    if req.method == "GET":

        return func.HttpResponse(
            get_html_content(),
            status_code=200,
            mimetype="text/html"
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
            src_container = parsed.get("src_container", [""])[0].strip().lower() + "/"
            dest_container = parsed.get("dest_container", [""])[0].strip().lower() + "/"
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

            # 3) Build the final JSON structure exactly as requested
            result = {
                "enabled": enabled,
                "name": name,
                "konserni": konserni_list,
                "source_container": src_container,
                "destination_container": dest_container,
                "file_format": file_format,
                "extra_columns": extra_columns,
                "exclude_columns": exclude_list
            }

            # Upload the JSON to Azure Blob Storage
            conf_stg = StorageHandler(container_name="asiakasrajapinnat")
            conf_stg.upload_blob(
                blob_name=f"CustomerConfig/{name}.json",
                data=json.dumps(result, ensure_ascii=False).encode("utf-8"),
                overwrite=True
            )

            # Create source directory
            src_stg = StorageHandler(container_name="vitecpowerbi")

            prefix = f"Rajapinta/{src_container}"
            history_dir = prefix + 'history/'

            marker = history_dir + '.keep'

            try:
                src_stg.upload_blob(marker, b'', overwrite=True)
                src_stg.container_client.delete_blob(marker)
            except Exception as e:
                # if something goes wrong it’s non‐fatal—just log it
                logging.error(f"Could not create directory marker {marker}: {e}")

            # Create destination container
            StorageHandler(container_name=dest_container)
            
            # 4) Return it as application/json
            return func.HttpResponse(
                json.dumps(result, ensure_ascii=False),
                status_code=200,
                mimetype="application/json"
            )
        except Exception as e:
            logging.error(f"Error processing POST request: {e}")
            return func.HttpResponse(
                json.dumps({"error": str(e)}),
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
