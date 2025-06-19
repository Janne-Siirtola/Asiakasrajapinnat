from config_page import form_parser
import sys
import os

# Ensure Azure Blob Storage connection string is available during imports.
# The Azure SDK does not accept the short ``UseDevelopmentStorage=true`` format
# so we provide a minimal Azurite style string instead.
os.environ.setdefault(
    "AzureWebJobsStorage",
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFeSClZg==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
)

# Ensure package can be imported
sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "Asiakasrajapinnat-AZFunction")))


def test_edit_base_columns_parsing(monkeypatch):
    body = (
        "method=edit_base_columns&"
        "key=foo&name=Foo&dtype=int&decimals=&"
        "key=bar&name=Bar&dtype=float&decimals=2"
    )
    method, result = form_parser.parse_form_data(body, [])
    assert method == "edit_base_columns"
    assert result == {
        "foo": {"name": "Foo", "dtype": "int"},
        "bar": {"name": "Bar", "dtype": "float", "decimals": 2},
    }


def test_create_customer_calls_create_containers(monkeypatch):
    called = {}

    def fake_create(src, dst, msgs):
        called["args"] = (src, dst)

    monkeypatch.setattr(form_parser, "create_containers", fake_create)
    body = (
        "method=create_customer&name=test&konserni=1,2&"
        "src_container=src&dest_container=dest&"
        "file_format=csv&file_encoding=utf-8&"
        "create_containers_check=true"
    )
    method, result = form_parser.parse_form_data(body, [])

    assert method == "create_customer"
    assert called["args"] == ("src/", "dest/")
    assert result["name"] == "test"
    assert result["konserni"] == [1, 2]
    assert result["source_container"] == "src/"
    assert result["destination_container"] == "dest/"
    assert result["file_format"] == "csv"
    assert result["file_encoding"] == "utf-8"


def test_delete_customer_parsing():
    body = "method=delete_customer&name=test"
    method, result = form_parser.parse_form_data(body, [])
    assert method == "delete_customer"
    assert result == "test"


def test_invalid_destination_container_name_is_rejected(monkeypatch):
    called = {}

    def fake_create(src, dst, msgs):
        called["called"] = True

    monkeypatch.setattr(form_parser, "create_containers", fake_create)
    body = (
        "method=create_customer&name=test&konserni=1&"
        "src_container=valid-src&dest_container=Invalid--Name&"
        "file_format=csv&file_encoding=utf-8&"
        "create_containers_check=true"
    )
    messages = []
    method, _ = form_parser.parse_form_data(body, messages)

    assert method == "create_customer"
    assert "called" not in called
    assert any(
        "Invalid destination container name" in m["message"] for m in messages
    )


def test_invalid_source_container_name_is_allowed(monkeypatch):
    called = {}

    def fake_create(src, dst, msgs):
        called["args"] = (src, dst)

    monkeypatch.setattr(form_parser, "create_containers", fake_create)
    body = (
        "method=create_customer&name=test&konserni=1&"
        "src_container=Invalid--Name&dest_container=validdest-123&"
        "file_format=csv&file_encoding=utf-8&"
        "create_containers_check=true"
    )
    messages = []
    method, _ = form_parser.parse_form_data(body, messages)

    assert method == "create_customer"
    assert called["args"] == ("invalid--name/", "validdest-123/")
    assert all("Invalid" not in m["message"] for m in messages)


def test_valid_container_names_are_accepted(monkeypatch):
    called = {}

    def fake_create(src, dst, msgs):
        called["args"] = (src, dst)

    monkeypatch.setattr(form_parser, "create_containers", fake_create)
    body = (
        "method=create_customer&name=test&konserni=1&"
        "src_container=valid-src&dest_container=validdest-123&"
        "file_format=csv&file_encoding=utf-8&"
        "create_containers_check=true"
    )
    messages = []
    method, result = form_parser.parse_form_data(body, messages)

    assert method == "create_customer"
    assert called["args"] == ("valid-src/", "validdest-123/")
    assert all("Invalid" not in m["message"] for m in messages)


def test_edit_customer_parsing():
    body = (
        "method=edit_customer&name=newname&original_name=oldname&"
        "konserni=1&src_container=src&dest_container=dest&"
        "file_format=csv&file_encoding=utf-8"
    )
    method, result = form_parser.parse_form_data(body, [])

    assert method == "edit_customer"
    assert result["name"] == "newname"
    assert result["original_name"] == "oldname"
