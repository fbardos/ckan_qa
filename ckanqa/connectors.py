import io
import logging
import os
from abc import abstractmethod
from dataclasses import dataclass
from typing import Generator, List, Tuple, TypeVar, Union

import boto3
import pandas as pd
import redis
import requests

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.redis.hooks.redis import RedisHook
from ckanqa.constant import (DEFAULT_REDIS_CONN_ID, DEFAULT_S3_CONN_ID,
                             S3_BUCKET_NAME_DATA)

BaseAwsConnection = TypeVar('BaseAwsConnection', bound=Union[boto3.client, boto3.resource])


@dataclass
class LoadResultContainer:
    csv: str  # Filepath of CSV
    dataframe: pd.DataFrame


# TODO: To start with, implement 3 connectors: S3 (minio), SFTP and Redis with all abstractmethods
class BaseConnector:

    def __init__(self, connection_id: str):
        self.connection_id = connection_id

    @abstractmethod
    def write_to_source_request(self, ckan_name: str, iso_8601_basic: str, filename: str, response: requests.Response):
        raise NotImplementedError

    @abstractmethod
    def write_to_source_buffer(self, ckan_name: str, iso_8601_basic: str, filename: str, buffer: io.BytesIO):
        raise NotImplementedError

    @abstractmethod
    def list_files_paths(self, path: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def load_from_source(self, ckan_id: str) -> List[LoadResultContainer]:
        raise NotImplementedError

    @abstractmethod
    def load_dataframe_from_filetype(self, **kwargs) -> Generator[Tuple[str, pd.DataFrame], None, None]:
        raise NotImplementedError

    @abstractmethod
    def delete_from_source(self, **kwrags) -> None:
        raise NotImplementedError

    @abstractmethod
    def _connect(self):
        raise NotImplementedError


# TODO: --> Currently not needed. But refactor for another connector.
# class SftpConnector(BaseConnector):

    # def __init__(
        # self,
        # store_path: Optional[str] = os.path.join('data', 'ckan'),
        # connection_id: str = DEFAULT_SFTP_CONN_ID,
    # ):
        # super().__init__(connection_id)
        # self._store_path = store_path

    # @property
    # def store_path(self):
        # if self._store_path is None:
            # raise AttributeError('Attribute store_path is not set.')
        # return self._store_path

    # def _connect(self) -> paramiko.SFTPClient:
        # client = SFTPHook(self.connection_id)
        # return client.get_conn()

    # def write_to_source(self, ckan_id: str, iso_8601_basic: str, filename: str, response: requests.Response):
        # # TODO: Use ckan_name instead of ckan_id for subdirectory
        # full_filepath = os.path.join(self.store_path, ckan_id)
        # with self._connect() as conn:
            # try:
                # conn.chdir(full_filepath)
            # except IOError:
                # conn.mkdir(full_filepath)
                # conn.chdir(full_filepath)
            # conn.putfo(io.BytesIO(response.content), f'{filename}.csv')

    # def load_from_source(self, ckan_id: str) -> List[LoadResultContainer]:
        # return_list = []
        # with self._connect() as conn:
            # conn.chdir(os.path.join(self.store_path, ckan_id))
            # for csv in conn.listdir():
                # logging.info(f'Loading {csv}...')
                # with io.BytesIO() as fl:
                    # conn.getfo(csv, fl)
                    # fl.seek(0)
                    # df = pd.read_csv(fl)
                    # if len(df.index) == 0:
                        # raise AttributeError('Loaded dataframe is empty.')
                # return_list.append(LoadResultContainer(csv, df))
        # return return_list

    # def delete_from_source(self, ckan_id: str) -> None:
        # with self._connect() as conn:
            # conn.chdir(self.store_path)
            # files = conn.listdir(ckan_id)
            # for file in files:
                # conn.remove(os.path.join(ckan_id, file))
            # conn.rmdir(ckan_id)


class RedisConnector(BaseConnector):

    def __init__(
        self,
        connection_id: str = DEFAULT_REDIS_CONN_ID,
    ):
        super().__init__(connection_id)

    def _connect(self) -> redis.client.Redis:
        hook = RedisHook(self.connection_id)
        return hook.get_conn()

    def write_to_source(self, ckan_id: str, iso_8601_basic: str, filename: str, response: requests.Response):
        # TODO: Maybe add timestamp as key.
        # TODO: Use ckan_name instead of ckan_id for identification.
        with self._connect() as conn:
            conn.set(f'ckan:csv:{ckan_id}:{filename}', response.content)

    # TODO: Is LoadResultContainer really needed?
    def load_from_source(self, ckan_id: str) -> List[LoadResultContainer]:
        return_list = []
        with self._connect() as conn:
            search_pattern = f'ckan:csv:{ckan_id}:*'
            if len(conn.keys(pattern=search_pattern)) > 0:  # Check if pattern exists
                for key in conn.scan_iter(search_pattern):
                    r = conn.get(key)
                    if r is None:
                        raise ValueError(f'Redis key {key} not found.')
                    filename = str(key).split(':')[-1] + '.csv'
                    fl = io.StringIO(r.decode('utf-8'))
                    df = pd.read_csv(fl)
                    if len(df.index) == 0:
                        raise AttributeError('Loaded dataframe is empty.')
                    return_list.append(LoadResultContainer(filename, df))
            else:
                raise ValueError(f'No keys found with pattern {search_pattern}')
        return return_list

    def delete_from_source(self, ckan_id: str) -> None:
        with self._connect() as conn:
            for key in conn.scan_iter(f'ckan:csv:{ckan_id}:*'):
                conn.delete(key)


class MinioConnector(BaseConnector):
    """Connector for Minoio (S3 object storage)"""
    # TODO: Why not use Airflow Hook architecture? Inherit from S3Hook?

    def __init__(self, connection_id: str = DEFAULT_S3_CONN_ID):
        super().__init__(connection_id)

    def _get_hook(self):
        return S3Hook(self.connection_id)

    def _connect(self) -> BaseAwsConnection:
        """

        Returns:
            boto3.client or boto3.resource

        """
        # TODO: Rename with get_connection()
        hook = self._get_hook()
        return hook.get_conn()

    def write_to_source_buffer(self, ckan_name: str, iso_8601_basic: str, filename: str, buffer: io.BytesIO):
        """ISO 8601 BASIC is needed as directory name."""
        conn = self._connect()
        buffer.seek(0)
        conn.put_object(
            Body=buffer,
            Bucket=S3_BUCKET_NAME_DATA,
            Key=os.path.join(ckan_name, iso_8601_basic, filename)
        )

    def write_to_source_request(self, ckan_name: str, iso_8601_basic: str, filename: str, response: requests.Response):
        file_object = io.BytesIO(response.content)
        self.write_to_source_buffer(ckan_name, iso_8601_basic, filename, file_object)

    def list_files_paths(self, path: str) -> List[str]:
        client = self._connect()
        list_objects = client.list_objects_v2(
            Bucket=S3_BUCKET_NAME_DATA,
            Prefix=path,
        )
        files = [i['Key'] for i in list_objects.get('Contents', [])]

        return files

    def load_from_directory(self, ckan_name: str, iso_8601_basic: str) -> Tuple[str, io.BytesIO]:
        """Loads all files files from directory."""
        client = self._connect()

        # List objects in subdirectory
        prefix = '/'.join([ckan_name, iso_8601_basic])

        for file in self.list_files_paths(prefix):
            obj_read = client.get_object(Bucket=S3_BUCKET_NAME_DATA, Key=file)
            yield file, io.BytesIO(obj_read['Body'].read())

    def load_dataframe_from_filetype(
        self, ckan_name: str, iso_8601_basic: str, filetype: str = '.csv'
    ) -> Generator[Tuple[str, pd.DataFrame], None, None]:
        """Returns an iterator with tuples of (path, pd.DataFrame)"""
        for file, buffer in self.load_from_directory(ckan_name, iso_8601_basic):

            # Skip files not ending with filetype parameter
            if not file.endswith(filetype):
                logging.info(
                    f'File {file} in path {"/".join([ckan_name, iso_8601_basic])} '
                    f'gets ignored, because not ending with {filetype}')
                continue

            # Yield pandas DataFrame from BytesIO object
            yield file, pd.read_csv(buffer)

    def delete_from_source(self, keys: Union[str, list]) -> None:
        hook = self._get_hook()
        hook.delete_objects(bucket=S3_BUCKET_NAME_DATA, keys=keys)
