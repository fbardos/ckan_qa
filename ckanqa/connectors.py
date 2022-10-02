import io
import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional

import pandas as pd
import paramiko
import redis
import requests

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from ckanqa.constant import DEFAULT_SFTP_CONN_ID

sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../ckanqa'))))
from ckanqa.constant import DEFAULT_REDIS_CONN_ID, DEFAULT_SFTP_CONN_ID


@dataclass
class LoadResultContainer:
    csv: str  # Filepath of CSV
    dataframe: pd.DataFrame


class BaseConnector:

    def __init__(self, connection_id: str):
        self.connection_id = connection_id

    @abstractmethod
    def write_to_source(self, ckan_id: str, filename: str, response: requests.Response):
        raise NotImplementedError

    @abstractmethod
    def load_from_source(self, ckan_id: str) -> List[LoadResultContainer]:
        raise NotImplementedError

    @abstractmethod
    def delete_from_source(self, ckan_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def _connect(self):
        raise NotImplementedError


class SftpConnector(BaseConnector):

    def __init__(
        self,
        store_path: Optional[str] = os.path.join('data', 'ckan'),
        connection_id: str = DEFAULT_SFTP_CONN_ID,
    ):
        super().__init__(connection_id)
        self._store_path = store_path

    @property
    def store_path(self):
        if self._store_path is None:
            raise AttributeError('Attribute store_path is not set.')
        return self._store_path

    def _connect(self) -> paramiko.SFTPClient:
        client = SFTPHook(self.connection_id)
        return client.get_conn()

    def write_to_source(self, ckan_id: str, filename: str, response: requests.Response):
        full_filepath = os.path.join(self.store_path, ckan_id)
        with self._connect() as conn:
            try:
                conn.chdir(full_filepath)
            except IOError:
                conn.mkdir(full_filepath)
                conn.chdir(full_filepath)
            conn.putfo(io.BytesIO(response.content), f'{filename}.csv')

    def load_from_source(self, ckan_id: str) -> List[LoadResultContainer]:
        return_list = []
        with self._connect() as conn:
            conn.chdir(os.path.join(self.store_path, ckan_id))
            for csv in conn.listdir():
                logging.info(f'Loading {csv}...')
                with io.BytesIO() as fl:
                    conn.getfo(csv, fl)
                    fl.seek(0)
                    df = pd.read_csv(fl)
                    if len(df.index) == 0:
                        raise AttributeError('Loaded dataframe is empty.')
                return_list.append(LoadResultContainer(csv, df))
        return return_list

    def delete_from_source(self, ckan_id: str) -> None:
        with self._connect() as conn:
            conn.chdir(self.store_path)
            files = conn.listdir(ckan_id)
            for file in files:
                conn.remove(os.path.join(ckan_id, file))
            conn.rmdir(ckan_id)


class RedisConnector(BaseConnector):

    def __init__(
        self,
        connection_id: str = DEFAULT_REDIS_CONN_ID,
    ):
        super().__init__(connection_id)

    def _connect(self) -> redis.client.Redis:
        hook = RedisHook(self.connection_id)
        return hook.get_conn()

    def write_to_source(self, ckan_id: str, filename: str, response: requests.Response):
        with self._connect() as conn:
            conn.set(f'ckan:csv:{ckan_id}:{filename}', response.content)

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

