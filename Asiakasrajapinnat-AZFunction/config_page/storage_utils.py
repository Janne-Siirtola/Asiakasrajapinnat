"""Blob storage helper functions used by the configuration page."""

import json
import logging
from typing import List

from azure.core.exceptions import AzureError

from asiakasrajapinnat_master.storage_handler import StorageHandler

from .utils import flash

# Storage handlers used across the configuration UI
src_stg = StorageHandler(container_name="vitecpowerbi")
conf_stg = StorageHandler(container_name="asiakasrajapinnat")


def create_containers(src_container: str, dest_container: str) -> None:
    """Create source and destination containers if they do not exist."""
    prefix = f"Rajapinta/{src_container}"
    history_dir = prefix + "history/"

    list_blobs = src_stg.list_blobs(prefix=prefix)
    if not list_blobs:
        try:
            marker = history_dir + ".keep"
            src_stg.upload_blob(marker, b"", overwrite=True)
            src_stg.container_client.delete_blob(marker)
        except AzureError as e:
            logging.error(
                "Could not create directory marker %s: %s",
                marker,
                e,
            )
    else:
        src_container = src_container.strip("/")
        flash(
            "error",
            f"Source container '{src_container}' already exists. "
            "Please choose a different name.",
        )

    dst_stg = StorageHandler(container_name=dest_container)
    if dst_stg.container_exists():
        dest_container = dest_container.strip("/")
        flash(
            "error",
            f"Destination container '{dest_container}' already exists. "
            "Please choose a different name.",
        )
    else:
        try:
            dst_stg.create_container()
            logging.info("Destination container '%s' created.", dest_container)
        except AzureError as e:
            flash("error", f"Failed to create destination container: {e}")
            logging.error("Failed to create destination container: %s", e)


def get_customers() -> List[str]:
    """Load customer configuration files from storage."""
    customers: List[str] = []
    try:
        for cfg_file in conf_stg.list_json_blobs("CustomerConfig"):
            try:
                raw = conf_stg.download_blob(cfg_file)
                data = json.loads(raw)
                customers.append(data)
            except (AzureError, json.JSONDecodeError) as e:
                logging.error("Failed to parse JSON from blob '%s': %s", cfg_file, e)
                continue
    except AzureError as e:
        logging.error("Failed to list blobs under CustomerConfig/: %s", e)
    return customers
