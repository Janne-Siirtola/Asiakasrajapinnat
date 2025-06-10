import logging
import azure.functions as func
import os
import json
from urllib.parse import parse_qs

from AsiakasrajapinnatMaster.StorageHandler import StorageHandler
from jinja2 import Environment, FileSystemLoader, select_autoescape

# version 1.18 (modified for “list all” + “edit single”)
module_dir = os.path.dirname(__file__)
templates_dir = os.path.join(module_dir, "templates")

jinja_env = Environment(
    loader=FileSystemLoader(templates_dir),
    autoescape=select_autoescape(["html", "xml"])
)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ServeConfig function processed a request.")

    conf_stg = StorageHandler(container_name="asiakasrajapinnat")

    # ────────────────
    # 1) Handle GET requests
    # ────────────────
    if req.method == "GET":
        # 1.a) If a specific customer “name” is given (for editing), load that JSON:
        name_to_edit = req.params.get("name")
        single_customer = None
        if name_to_edit:
            blob_path = f"CustomerConfig/{name_to_edit}.json"
            try:
                raw_json = conf_stg.download_blob(blob_path)
                single_customer = json.loads(raw_json)
            except Exception as e:
                logging.warning(f"Could not load customer '{name_to_edit}': {e}")
                single_customer = None  # leave it None if not found

        # 1.b) Regardless, load _all_ customer JSONs into a list
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
                    logging.error(f"Failed to parse JSON from blob '{cfg_file}': {e}")
                    continue
        except Exception as e:
            logging.error(f"Failed to list blobs under CustomerConfig/: {e}")

        # 1.c) Render the same template, passing both “customers” list and optional “single_customer”
        template = jinja_env.get_template("index.html")
        rendered = template.render(
            customers=customers,
            customer=single_customer,
            function_key=os.getenv("function_key")
        )
        return func.HttpResponse(
            rendered,
            status_code=200,
            mimetype="text/html"
        )

    # ────────────────────────────
    # 2) Handle POST (create / update)
    # ────────────────────────────
    elif req.method == "POST":
        try:
            # 2.a) Parse form-encoded body
            try:
                raw_body = req.get_body().decode("utf-8")
                parsed = parse_qs(raw_body)
            except Exception as e:
                logging.error(f"Failed to parse POST body: {e}")
                return func.HttpResponse(
                    json.dumps({"error": "Could not parse form data"}),
                    status_code=400,
                    mimetype="application/json"
                )

            # 2.b) Build the JSON structure
            enabled = True

            name = parsed.get("name", [""])[0].strip().lower()
            if not name:
                return func.HttpResponse(
                    json.dumps({"error": "Name is required"}),
                    status_code=400,
                    mimetype="application/json"
                )

            konserni_raw = parsed.get("konserni", [""])[0].strip()
            konserni_list = []
            if konserni_raw:
                for part in konserni_raw.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    try:
                        konserni_list.append(int(part))
                    except ValueError:
                        logging.warning(f"Ignoring non-numeric konserni token: '{part}'")
                        continue

            src_container = parsed.get("src_container", [""])[0].strip().lower() + "/"
            dest_container = parsed.get("dest_container", [""])[0].strip().lower() + "/"
            file_format = parsed.get("file_format", [""])[0].strip().lower()

            extra_keys = parsed.get("extra_key", [])
            extra_names = parsed.get("extra_name", [])
            extra_dtypes = parsed.get("extra_dtype", [])
            extra_columns = {}
            for key, disp, dt in zip(extra_keys, extra_names, extra_dtypes):
                key = key.strip()
                disp = disp.strip()
                dt = dt.strip()
                if key:
                    extra_columns[key] = {
                        "name": disp,
                        "dtype": dt
                    }

            exclude_raw = parsed.get("exclude_columns", [""])[0].strip()
            exclude_list = []
            if exclude_raw:
                for part in exclude_raw.split(","):
                    part = part.strip()
                    if part:
                        exclude_list.append(part)

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

            # 2.c) Upload (overwrite if it already exists)
            conf_stg.upload_blob(
                blob_name=f"CustomerConfig/{name}.json",
                data=json.dumps(result, ensure_ascii=False).encode("utf-8"),
                overwrite=True
            )

            # 2.d) (Rest of your logic to create directories, etc.)
            src_stg = StorageHandler(container_name="vitecpowerbi")
            prefix = f"Rajapinta/{src_container}"
            history_dir = prefix + "history/"
            marker = history_dir + ".keep"
            try:
                src_stg.upload_blob(marker, b"", overwrite=True)
                src_stg.container_client.delete_blob(marker)
            except Exception as e:
                logging.error(f"Could not create directory marker {marker}: {e}")

            StorageHandler(container_name=dest_container)

            # 2.e) Return the newly saved JSON
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

    # ──────────────────
    # 3) Other verbs → 405
    # ──────────────────
    else:
        return func.HttpResponse(
            "Method Not Allowed",
            status_code=405,
            mimetype="text/plain"
        )
