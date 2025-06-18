"""Helper functions for rendering templates and managing flash messages."""

import os
import hmac
import hashlib
import secrets
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

import azure.functions as func
from jinja2 import Environment, FileSystemLoader, select_autoescape

module_dir = os.path.dirname(__file__)
templates_dir = os.path.join(module_dir, "templates")
static_dir = os.path.join(module_dir, "static")
css_dir = os.path.join(static_dir, "css")
js_dir = os.path.join(static_dir, "js")

jinja_env = Environment(
    loader=FileSystemLoader(templates_dir),
    autoescape=select_autoescape(["html", "xml"]),
)

# Secret used for signing CSRF tokens. This must be provided
# via environment variables. Fail fast if it's missing so that
# misconfiguration doesn't silently disable protection.
CSRF_SECRET = os.environ.get("CSRF_SECRET")
if not CSRF_SECRET:
    raise RuntimeError("CSRF_SECRET environment variable is not set")

def flash(messages: List[Dict[str, str]], category: str = "error", message: str = "") -> None:
    """Append a flash message to ``messages``."""
    messages.append({"category": category, "message": message})


def render_template(context: Dict[str, Any]) -> func.HttpResponse:
    """Render a Jinja2 template using ``context``."""
    template_name = context.get("template_name", "")
    status_code = context.get("status_code", 200)
    mimetype = context.get("mimetype", "text/html")
    headers = context.get("headers")

    render_args = context.copy()
    messages = render_args.get("messages") or []
    render_args["messages"] = messages
    render_args.pop("template_name", None)
    render_args.pop("status_code", None)
    render_args.pop("mimetype", None)

    template = jinja_env.get_template(template_name)
    rendered = template.render(**render_args)
    return func.HttpResponse(
        rendered,
        status_code=status_code,
        mimetype=mimetype,
        headers=headers,
    )


def _read_files(base_dir: str, files: List[str]) -> List[str]:
    blocks: List[str] = []
    for name in files:
        path = os.path.join(base_dir, name)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                blocks.append(fh.read())
    return blocks


def get_css_blocks(file_specific_styles: Optional[List[str]] = None) -> List[str]:
    """Return CSS snippets for the page."""
    global_styles = ["base.css", "flash.css", "navbar.css"]
    css_blocks = _read_files(css_dir, global_styles)
    if file_specific_styles:
        css_blocks.extend(_read_files(css_dir, file_specific_styles))
    return css_blocks


def get_js_blocks(file_specific_scripts: Optional[List[str]] = None) -> List[str]:
    """Return JavaScript snippets for the page."""
    global_scripts = ["navbar.js", "flash.js"]
    js_blocks = _read_files(js_dir, global_scripts)
    if file_specific_scripts:
        js_blocks.extend(_read_files(js_dir, file_specific_scripts))
    return js_blocks


def get_html_blocks(file_specific_html: Optional[List[str]] = None) -> List[Dict[str, str]]:
    """Return HTML template fragments to include in the page."""
    html_blocks: List[Dict[str, str]] = []
    global_html = ["navbar.html"]

    for name in global_html:
        path = os.path.join(templates_dir, name)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                html_blocks.append({"name": name, "content": fh.read()})

    if file_specific_html:
        for name in file_specific_html:
            path = os.path.join(templates_dir, name)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as fh:
                    html_blocks.append({"name": name, "content": fh.read()})

    return html_blocks


def _sign(value: str) -> str:
    """Return HMAC signature for ``value`` using ``CSRF_SECRET``."""
    return hmac.new(CSRF_SECRET.encode(), value.encode(), hashlib.sha256).hexdigest()


def generate_csrf_token() -> tuple[str, str]:
    """Return a new token and the cookie value to store server-side."""
    token = secrets.token_urlsafe(32)
    signature = _sign(token)
    logging.info("Generated CSRF token")
    return token, f"{token}|{signature}"


def validate_csrf_token(form_token: str, cookie_value: str) -> bool:
    """Validate the CSRF token from the form against the cookie value."""
    if not form_token or not cookie_value:
        return False
    try:
        token, signature = cookie_value.split("|", 1)
    except ValueError:
        return False
    if not hmac.compare_digest(_sign(token), signature):
        return False
    return hmac.compare_digest(form_token, token)


def parse_cookie(cookie_header: str) -> Dict[str, str]:
    """Simple cookie parser returning a mapping of cookie names to values."""
    cookies: Dict[str, str] = {}
    if not cookie_header:
        return cookies
    parts = cookie_header.split(";")
    for part in parts:
        if "=" in part:
            name, val = part.split("=", 1)
            cookies[name.strip()] = unquote(val.strip())
    return cookies
