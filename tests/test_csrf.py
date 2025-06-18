import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Asiakasrajapinnat-AZFunction")))

# Ensure Azure Blob Storage connection string is available for imports
os.environ.setdefault(
    "AzureWebJobsStorage",
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFeSClZg==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
)

os.environ["CSRF_SECRET"] = "test-secret"
from config_page import utils


def test_validate_csrf_token_valid():
    token, cookie = utils.generate_csrf_token()
    assert utils.validate_csrf_token(token, cookie)


def test_validate_csrf_token_invalid():
    token, cookie = utils.generate_csrf_token()
    assert not utils.validate_csrf_token('wrong', cookie)
    assert not utils.validate_csrf_token(token, 'bogus')
