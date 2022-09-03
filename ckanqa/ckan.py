import datetime as dt
import io
import json
import os
import re
from typing import List, Optional

import requests

from airflow.models.baseoperator import BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

_DEFAULT_SFTP_CONN_ID = 'sftp_bucket'


class CkanCsvStoreOperator(BaseOperator):
    """Stores CSVs on remote location via SFTP.

    This operator will create a directory (named after CKAN ID) and store
    all CSVs in this directory, under the specified store_path.

    Args:
        ckan_metadata_url: URL to CKAN package metadata.
        extract_csv_urls: List of URLs to CSVs to extract and store.
            If not set, the operator will download all CSVs from CKAN package.
        sftp_connection_id: Connection ID for SFTP, used by Airflow.
        store_path: Path to store on destination (SFTP).
        **kwargs: Kwargs for Airflow task context.

    """

    def __init__(
        self,
        ckan_metadata_url: str,
        extract_csv_urls: Optional[List[str]] = None,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        **kwargs
    ):
        super().__init__(**kwargs)
        self.ckan_metadata_url = ckan_metadata_url
        self.extract_csv_urls = extract_csv_urls
        self.sftp_connection_id = sftp_connection_id
        self.store_path = store_path

        # Extract metadata from ckan
        r = requests.get(self.ckan_metadata_url)
        self._meta = json.loads(r.text)
        self.ckan_name = self._meta['result']['name']
        self.ckan_id = self._meta['result']['id']

        # Set general attributes
        self.timestamp = dt.datetime.now(tz=dt.timezone.utc)
        self.timestamp_str = self.timestamp.strftime("%Y-%m-%d")
        self.remote_filepath = os.path.join(self.store_path, self.ckan_id)

    def execute(self, context):
        if self.extract_csv_urls is None:
            csv_urls = [i['download_url'] for i in self._meta['result']['resources'] if i['media_type'] == 'text/csv']
        else:
            csv_urls = self.extract_csv_urls

        # Retrieve CSVs and store to NAS
        sftp_client = SFTPHook(self.sftp_connection_id)
        conn = sftp_client.get_conn()
        try:
            try:
                conn.chdir(self.remote_filepath)
            except IOError:
                conn.mkdir(self.remote_filepath)
                conn.chdir(self.remote_filepath)
            for csv in csv_urls:
                r = requests.get(csv)
                original_filename = re.findall(r'([-a-zA-Z0-9_]+)\.csv', csv)[0]
                conn.putfo(io.BytesIO(r.content), f'{self.timestamp_str}-{original_filename}.csv')
        finally:
            sftp_client.close_conn()


class CkanCsvDeleteOperator(BaseOperator):
    """Deletes stored files on remote location via SFTP.

    This operator will delete all files on remote location's path,
    and the directory (named after CKAN ID) itself.

    Args:
        ckan_metadata_url: URL to CKAN package metadata.
        sftp_connection_id: Connection ID for SFTP, used by Airflow.
        store_path: Path to store on destination (SFTP).
        **kwargs: Kwargs for Airflow task context.

    """

    def __init__(
        self,
        ckan_metadata_url: str,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        **kwargs
    ):
        super().__init__(**kwargs)
        self.ckan_metadata_url = ckan_metadata_url
        self.sftp_connection_id = sftp_connection_id
        self.store_path = store_path

        # Extract metadata from ckan
        r = requests.get(self.ckan_metadata_url)
        self._meta = json.loads(r.text)
        self.ckan_id = self._meta['result']['id']
        self.remote_filepath = os.path.join(self.store_path, self.ckan_id)

    def execute(self, context):

        # Delete filese in directory on NAS
        sftp_client = SFTPHook(self.sftp_connection_id)
        conn = sftp_client.get_conn()
        try:
            conn.chdir(self.store_path)

            # Delete files, than the directory itself.
            files = conn.listdir(self.ckan_id)
            for file in files:
                conn.remove(os.path.join(self.ckan_id, file))
            conn.rmdir(self.ckan_id)
        finally:
            sftp_client.close_conn()

