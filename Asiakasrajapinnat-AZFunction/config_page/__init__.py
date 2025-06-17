"""HTTP endpoint serving the configuration UI and form processing."""

import logging

import azure.functions as func

from .handlers import handle_get, handle_post
from .utils import flash_messages

__all__ = ["main"]


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function entry point for serving and processing the form."""
    logging.info("ServeConfig function processed a request.")

    flash_messages.clear()  # Clear previous flash messages

    if req.method == "GET":
        return handle_get(req)
    if req.method == "POST":
        return handle_post(req)
    return func.HttpResponse(
        "Method Not Allowed",
        status_code=405,
        mimetype="text/plain",
    )
