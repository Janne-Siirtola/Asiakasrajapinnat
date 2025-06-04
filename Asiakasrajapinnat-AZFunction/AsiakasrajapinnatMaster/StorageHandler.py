from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from typing import List, Optional
import os
import logging


class StorageHandler:
    def __init__(self, container_name: str, log_func=None) -> None:
        connection_str = os.environ["AzureWebJobsStorage"]
        self.container_name = container_name
        self.blob_service = BlobServiceClient.from_connection_string(connection_str)
        self.container_client: ContainerClient = self.blob_service.get_container_client(container_name)
        self.log = log_func
        
        if self.log is None:
            self.log = logging.info

        # make sure the container exists; if not, create it
        if not self.container_client.exists():
            self.container_client.create_container()
            if self.log:
                self.log(f"Created container '{container_name}'")
        else:
            if self.log:
                self.log(f"Using existing container '{container_name}'")

    def list_csv_blobs(self, prefix: Optional[str] = None) -> List[str]:
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        return [b.name for b in blobs if b.name.lower().endswith(".csv")]

    def list_json_blobs(self, prefix: Optional[str] = None) -> List[str]:
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        return [b.name for b in blobs if b.name.lower().endswith(".json")]

    def download_blob(self, blob_name: str) -> bytes:
        blob_client: BlobClient = self.container_client.get_blob_client(
            blob_name)
        return blob_client.download_blob().readall()

    def upload_blob(self, blob_name: str, data: bytes, overwrite: bool = True) -> None:
        blob_client: BlobClient = self.container_client.get_blob_client(
            blob_name)
        blob_client.upload_blob(data, overwrite=overwrite)

    def move_file_to_dir(
        self,
        source_blob_name: str,
        target_dir: str,
        overwrite: bool = True
    ) -> str:
        """
        Move a blob within this container to a different "directory" (i.e. prefix).

        :param source_blob_name: full path/name of the blob to move (e.g. 'inbound/data.csv')
        :param target_dir:    destination prefix (e.g. 'processed/')
        :param overwrite:     if True, will overwrite any existing blob at the destination
        :return:              the full blob name at the new location
        """
        # normalize destination prefix so it ends with exactly one '/'
        dest_dir = target_dir.rstrip('/') + '/'

        # extract just the filename (after any '/')
        filename = os.path.basename(source_blob_name)

        # build the new blob name
        dest_blob_name = f"{dest_dir}{filename}"

        # 1) download the original
        data = self.download_blob(source_blob_name)

        # 2) upload to the new location
        self.upload_blob(dest_blob_name, data, overwrite=overwrite)

        # 3) delete the original
        self.container_client.delete_blob(source_blob_name)
        self.log(f"Moved blob from {self.container_name}/{source_blob_name} to {self.container_name}/{dest_blob_name}")

        return dest_blob_name
