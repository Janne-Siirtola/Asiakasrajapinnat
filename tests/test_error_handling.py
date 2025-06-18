import os
import sys
import json
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Asiakasrajapinnat-AZFunction")))

os.environ.setdefault("CSRF_SECRET", "test-secret")
os.environ.setdefault(
    "AzureWebJobsStorage",
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFeSClZg==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
)

from config_page import handlers
from config_page.exceptions import InvalidInputError, ClientError
from config_page import form_parser


def test_handle_error_returns_generic_message():
    resp = handlers.handle_error(Exception("boom"))
    body = json.loads(resp.get_body().decode())
    assert body == {"error": "Internal server error"}


def test_invalid_method_raises_error():
    with pytest.raises(InvalidInputError):
        form_parser.parse_form_data("method=unknown", [])
        
    with pytest.raises(ClientError):
        handlers.prepare_template_context(method="unknown")
