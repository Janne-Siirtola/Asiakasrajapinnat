"""Form data parsing utilities for the configuration page."""

import json
import logging
import re
from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qs


from .storage_utils import create_containers
from .utils import flash
from .exceptions import InvalidInputError


def is_valid_container_name(name: str) -> bool:
    """Return True if ``name`` is a valid Azure container name."""
    if not 3 <= len(name) <= 63:
        return False
    return re.match(r"^(?!.*--)[a-z0-9](?:[a-z0-9-]*[a-z0-9])$", name) is not None


def _parse_base_columns(
    parsed: Dict[str, List[str]], messages: List[Dict[str, str]]
) -> Dict[str, Dict[str, Any]]:
    """Extract base column configuration from parsed form data."""
    keys = parsed.get("key", [])
    names = parsed.get("name", [])
    dtypes = parsed.get("dtype", [])
    decimals = parsed.get("decimals", [])
    lengths = parsed.get("length", [])

    basecols: Dict[str, Dict[str, Any]] = {}
    for k, n, dt, dec, length in zip(keys, names, dtypes, decimals, lengths):
        k = k.strip()
        if not k:
            continue
        col = {"name": n.strip(), "dtype": dt.strip()}
        if dt.strip() == "float" and dec.strip():
            try:
                col["decimals"] = int(dec)
            except ValueError:
                flash(messages, "error",
                      f"Invalid decimal value for column '{k}': {dec.strip()}")
        if dt.strip() == "string" and length.strip():
            try:
                col["length"] = int(length)
            except ValueError:
                flash(messages, "error",
                      f"Invalid length value for column '{k}': {length.strip()}")
        basecols[k] = col
    return basecols


def _parse_konserni_list(raw_value: str, messages: List[Dict[str, str]]) -> List[int]:
    """Parse a comma separated list of konserni ids."""
    konserni_list: List[int] = []
    for part in filter(None, [p.strip() for p in raw_value.split(",")]):
        try:
            konserni_list.append(int(part))
        except ValueError:
            logging.warning("Ignoring non-numeric konserni token: '%s'", part)
            flash(messages, "error", f"Invalid konserni value: '{part}'. "
                  "Please enter numeric values only.")
    return konserni_list

def _parse_email_list(raw_value: str) -> List[str]:
    """Parse a comma separated list of email addresses."""
    emails = [email.strip() for email in raw_value.split(",") if email.strip()]
    if not emails:
        raise InvalidInputError("Email list cannot be empty.")
    return emails


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


def _parse_enabled(method: str, parsed: Dict[str, List[str]]) -> bool:
    """Determine the enabled state for the configuration."""
    if method == "create_customer":
        return True
    return parsed.get("enabled", [""])[0].strip().lower() == "true"


def _parse_containers(
    parsed: Dict[str, List[str]], messages: List[Dict[str, str]]
) -> Tuple[str, str, str, str]:
    """Extract and validate container related values from parsed form data."""
    src_container = parsed.get("src_container", [""])[0].strip().lower()
    dest_container = parsed.get("dest_container", [""])[0].strip().lower()
    file_format = parsed.get("file_format", [""])[0].strip().lower()
    file_encoding = parsed.get("file_encoding", [""])[0].strip().lower()

    # Source container validation is unnecessary since it's not a container, 
    # but a directory within the container.
    
    if dest_container and not is_valid_container_name(dest_container):
        flash(
            messages,
            "error",
            f"Invalid destination container name '{dest_container}'.",
        )

    if src_container:
        src_container += "/"
    if dest_container:
        dest_container += "/"

    return src_container, dest_container, file_format, file_encoding


def _build_result(
    enabled: bool,
    name: str,
    konserni_list: List[int],
    src_container: str,
    dest_container: str,
    file_format: str,
    file_encoding: str,
    extra_columns: Dict[str, Dict[str, str]],
    exclude_list: List[str],
) -> Dict[str, Any]:
    """Assemble the configuration dictionary."""
    return {
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


def parse_form_data(
    body: str,
    messages: List[Dict[str, str]],
) -> Tuple[str, Any]:
    """Parse POSTed form data and return method and configuration."""
    parsed = parse_qs(body, keep_blank_values=True)

    method = parsed.get("method", [""])[0].strip().lower()
    logging.info("Form method received: %s", method)
    if method == "edit_base_columns":
        basecols = _parse_base_columns(parsed, messages)
        email_raw = parsed.get("emails", [""])[0].strip()
        emails = _parse_email_list(email_raw)
        result = {
            "base_columns": basecols,
            "emails": emails,
        }
        return method, result

    if method == "delete_customer":
        name = parsed.get("name", [""])[0].strip().lower()
        return method, name

    if method == "update_enabled":
        statuses_raw = parsed.get("statuses", ["{}"])[0]
        try:
            statuses = json.loads(statuses_raw) if statuses_raw else {}
        except json.JSONDecodeError as exc:
            raise InvalidInputError("Invalid statuses") from exc
        return method, statuses

    if method not in ["create_customer", "edit_customer"]:
        raise InvalidInputError("Invalid method")

    enabled = _parse_enabled(method, parsed)
    name = parsed.get("name", [""])[0].strip().lower()
    original_name = parsed.get("original_name", [""])[0].strip().lower()
    konserni_raw = parsed.get("konserni", [""])[0].strip()
    konserni_list = _parse_konserni_list(konserni_raw, messages)
    src_container, dest_container, file_format, file_encoding = _parse_containers(
        parsed, messages)
    extra_columns = _parse_extra_columns(parsed)
    exclude_list = parsed.get("exclude_columns", [])

    if method == "create_customer":
        check_str = parsed.get("create_containers_check", [""])[
            0].strip().lower()
        if (
            check_str == "true"
            and is_valid_container_name(dest_container.strip("/"))
        ):
            create_containers(src_container, dest_container, messages)

    result = _build_result(
        enabled,
        name,
        konserni_list,
        src_container,
        dest_container,
        file_format,
        file_encoding,
        extra_columns,
        exclude_list,
    )

    if method == "edit_customer":
        result["original_name"] = original_name

    logging.info("Parsed data for customer '%s'", name)

    return method, result
