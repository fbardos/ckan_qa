import io
import json
import os
from abc import ABC, abstractmethod
from typing import Generator, List, Tuple, TypeVar, Union

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BaseAwsConnection = TypeVar('BaseAwsConnection', bound=Union[boto3.client, boto3.resource])

load_dotenv()
DEFAULT_S3_CONN_ID = os.environ['CKANQA__CONFIG__S3_CONN_ID']


class FilesystemBaseConnector(ABC):
    """Abstract class for connectors to filesystem like SFTP or Minio."""

    def __init__(self, connection_id: str):
        self.connection_id = connection_id

    # @abstractmethod
    # def _connect(self):
        # raise NotImplementedError

    @abstractmethod
    def load_from_target(self, filepath: str, filename: str | None = None) -> io.BytesIO:
        """cRud"""
        raise NotImplementedError

    @abstractmethod
    def load_directory_from_target(self, filepath: str) -> Generator[Tuple[str, io.BytesIO], None, None]:
        """cRud, loads all the files from a directory. Returns a Generator."""
        raise NotImplementedError

    @abstractmethod
    def load_dataframe_from_target(self, filepath: str, filename: str | None = None) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def list_path_objects(self, path: str) -> List[str]:
        """cRud list"""
        raise NotImplementedError

    @abstractmethod
    def write_to_target(self, filepath: str, buffer: io.BytesIO, filename: str | None = None) -> None:
        """Crud"""
        raise NotImplementedError

    @abstractmethod
    def write_to_target_request_content(self, filepath: str, response: requests.Response, filename: str | None = None):
        """Crud"""
        raise NotImplementedError

    @abstractmethod
    def delete_from_target(self, filepath: str, filename: str | None = None) -> None:
        """cruD"""
        raise NotImplementedError


class MinioConnector(FilesystemBaseConnector):
    """Connector for Minoio (S3 object storage)"""

    def __init__(self, bucket_name: str, connection_id: str = DEFAULT_S3_CONN_ID):
        super().__init__(connection_id)
        self.hook = S3Hook(self.connection_id)
        self.bucket_name = bucket_name  # Name of the bucket is only needed inside this connector

    @property
    def boto3_options(self) -> dict:

        # S3Hook does not store login, password or additional config.
        # Use airflow.hooks.base.BaseHook instead.
        connection = self.hook.get_connection(self.connection_id)
        return {
            'endpoint_url': json.loads(connection.get_extra())['endpoint_url'],
            'aws_access_key_id': connection.login,
            'aws_secret_access_key': connection._password,
        }

    def _generate_key(self, filepath: str, filename: str | None = None) -> str:
        """When no filename is given, then assumed that filepath is the full path, including filename"""
        return '/'.join(filter(None, [filepath, filename]))

    def _load_from_target_by_key(self, key: str) -> io.BytesIO:
        response = self.hook.read_key(key, bucket_name=self.bucket_name)  # loads as string
        buffer = io.BytesIO(bytes(response, 'utf-8'))
        return buffer

    def load_from_target(self, filepath: str, filename: str | None = None) -> io.BytesIO:
        """cRud"""
        key = self._generate_key(filepath, filename)
        return self._load_from_target_by_key(key)

    def load_directory_from_target(self, filepath: str) -> Generator[Tuple[str, io.BytesIO], None, None]:
        """cRud directory, loads all the files from a directory. Returns a Generator."""
        for key in self.list_path_objects(filepath):
            obj_read = self.load_from_target(key)
            yield key, obj_read

    def load_dataframe_from_target(self, filepath: str, filename: str | None = None) -> pd.DataFrame:
        """Returns pandas.DataFrame from e.g. csv or parquet files."""
        key = self._generate_key(filepath, filename)
        buffer = self.load_from_target(key)
        match os.path.splitext(key)[1]:  # match file ending
            case '.csv':
                return pd.read_csv(buffer)
            case '.parquet':
                return pd.read_parquet(buffer)
            case _:
                raise ValueError(
                    f'Prefix {key} '
                    f'gets ignored, because it\'s not ending with either .csv or .parquet.'
                )

    def list_path_objects(self, path: str) -> List[str]:
        return self.hook.list_keys(self.bucket_name, path)

    def write_to_target(self, filepath: str, buffer: io.BytesIO, filename: str | None = None) -> None:
        """Crud"""
        key = self._generate_key(filepath, filename)
        buffer.seek(0)
        self.hook.load_bytes(buffer.read(), key, bucket_name=self.bucket_name, replace=True)

    def write_to_target_request_content(self, filepath: str, response: requests.Response, filename: str | None = None):
        # buffer = io.BytesIO(response.content)
        buffer = io.BytesIO()
        buffer.write(response.content)
        key = self._generate_key(filepath, filename)
        self.write_to_target(key, buffer)

    def delete_from_target(self, filepath: str, filename: str | None = None) -> None:
        """cruD"""
        key = self._generate_key(filepath, filename)
        self.hook.delete_objects(self.bucket_name, key)
