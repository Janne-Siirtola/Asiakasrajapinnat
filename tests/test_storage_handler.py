import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Asiakasrajapinnat-AZFunction")))

from asiakasrajapinnat_master import storage_handler


def test_move_file_to_dir():
    handler = storage_handler.StorageHandler.__new__(storage_handler.StorageHandler)
    handler.container_name = "cont"
    handler.download_blob = MagicMock(return_value=b"data")
    handler.upload_blob = MagicMock()
    handler.container_client = MagicMock()

    dest = handler.move_file_to_dir("in/file.csv", "processed")

    assert dest == "processed/file.csv"
    handler.download_blob.assert_called_once_with("in/file.csv")
    handler.upload_blob.assert_called_once_with("processed/file.csv", b"data", overwrite=True)
    handler.container_client.delete_blob.assert_called_once_with("in/file.csv")

