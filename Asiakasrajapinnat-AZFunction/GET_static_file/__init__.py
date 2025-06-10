import logging
import os
import mimetypes

import azure.functions as func

# 1) Determine where "static/" lives relative to this file
static_dir = os.path.join("static")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP trigger processed a request to serve a static file.")

    # 2) Read the "path" parameter from query string or JSON body
    file_path_param = req.params.get("path")
    if not file_path_param:
        try:
            body = req.get_json()
        except ValueError:
            body = {}
        file_path_param = body.get("path")

    if not file_path_param:
        return func.HttpResponse(
            "Please pass a 'path' query parameter (e.g. ?path=css/styles.css)",
            status_code=400
        )

    # 3) Normalize the requested path to prevent directory‐traversal
    #    e.g. "css/../secrets.txt" → "../secrets.txt"
    normalized = os.path.normpath(file_path_param)
    # If normalized begins with ".." or is absolute, reject it
    if normalized.startswith("..") or os.path.isabs(normalized):
        return func.HttpResponse(
            "Invalid file path.",
            status_code=400
        )

    # 4) Build the full filesystem path under static/
    #    e.g. static/css/styles.css
    full_path = os.path.join(static_dir, normalized)

    # 5) Check file existence
    if not os.path.isfile(full_path):
        return func.HttpResponse(
            f"File not found: {file_path_param}",
            status_code=404
        )

    # 6) Guess the MIME type based on extension
    mime_type, _ = mimetypes.guess_type(full_path)
    if mime_type is None:
        mime_type = "application/octet-stream"

    # 7) Read the file bytes and return
    try:
        with open(full_path, "rb") as f:
            content = f.read()
    except Exception as e:
        logging.error(f"Error reading file {full_path}: {e}")
        return func.HttpResponse(
            "Error reading the requested file.",
            status_code=500
        )

    return func.HttpResponse(
        body=content,
        status_code=200,
        mimetype=mime_type
    )
