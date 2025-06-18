"""HTTP endpoint serving the configuration UI and form processing."""

import logging

import azure.functions as func

from .handlers import handle_get, handle_post

__all__ = ["main"]


# Silence the Blob SDK’s HTTP logs
logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function entry point for serving and processing the form."""
    logging.info("config_page function processed a request.")

    if req.method == "GET":
        return handle_get(req)
    if req.method == "POST":
        return handle_post(req)
    return func.HttpResponse(
        "Method Not Allowed",
        status_code=405,
        mimetype="text/plain",
    )
