import sys
import os

# Ensure package can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Asiakasrajapinnat-AZFunction")))

from config_page import form_parser


def test_edit_basecols_parsing(monkeypatch):
    body = (
        "method=edit_basecols&"
        "key=foo&name=Foo&dtype=int&decimals=&"
        "key=bar&name=Bar&dtype=float&decimals=2"
    )
    method, result = form_parser.parse_form_data(body)
    assert method == "edit_basecols"
    assert result == {
        "foo": {"name": "Foo", "dtype": "int"},
        "bar": {"name": "Bar", "dtype": "float", "decimals": 2},
    }


def test_create_customer_calls_create_containers(monkeypatch):
    called = {}

    def fake_create(src, dst):
        called["args"] = (src, dst)

    monkeypatch.setattr(form_parser, "create_containers", fake_create)
    body = (
        "method=create_customer&name=test&konserni=1,2&"
        "src_container=src&dest_container=dest&"
        "file_format=csv&file_encoding=utf-8&"
        "create_containers_check=true"
    )
    method, result = form_parser.parse_form_data(body)

    assert method == "create_customer"
    assert called["args"] == ("src/", "dest/")
    assert result["name"] == "test"
    assert result["konserni"] == [1, 2]
    assert result["source_container"] == "src/"
    assert result["destination_container"] == "dest/"
    assert result["file_format"] == "csv"
    assert result["file_encoding"] == "utf-8"


