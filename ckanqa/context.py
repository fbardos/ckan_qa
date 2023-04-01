from __future__ import annotations

import datetime as dt
import inspect
import json
import logging
import os
from typing import Any, List, Optional

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.redis.hooks.redis import RedisHook
from ckanqa.hook import CkanInstanceHook
from ckanqa.operator.base import CkanBaseOperator


class CkanContext:
    """Context for operations with GE for CKANQA

    This context stores objects, such as BatchReques, datasources etc to use
    in different Airflow operator. Is used to pass different states along
    multiple Airflow operators.

    Usually initialized in a DAG file. Gets passed as parameter for
    differenct CkanBaseOperator.

    Context cannot contain any objects e.g. from boto3. Otherwise,
    will get problems during pickle creation. Instead, use context to store configs.

    One checkpont_config contains different configs for a complete GE run:
        self.checkpoint_config = {
            'checkpoint_name': {
                'suite': {
                    'suite_config': {...}
                    -- 'expectations': [{...}, {...}] --> does not contain any expectations anymore.
                }
                'batch_request':
                    'batch_request_config': {...}
                    'datasource_config': {...}
            }
        }

    """
    REDIS_KEY_PARTS = ('ckan', 'context')
    PERSISTED_PROPERTIES = [
        'ckan_name',
        'dag_runtime',
        'ckan_api_base_url',
        'connector_class',
        'airflow_connection_id',
        'data_bucket_name',
        'checkpoint_success',  # Tracks success of different checkpoints
    ]

    def __init__(
        self,
        ckan_name: str,  # Identifier for redis key
        dag_runtime: Optional[dt.datetime] = None,
        import_from_redis: bool = False,
        ckan_api_base_url: str = 'https://ckan.opendata.swiss',
        connector_class: str = 'MinioConnector',
        airflow_connection_id: str | None = None,
        data_bucket_name: str | None = None,
    ):

        # General, part of REDIS keys
        self.ckan_name = ckan_name
        self._dag_runtime = dag_runtime  # Can be set after init

        self.REDIS_CONN_ID = Variable.get('CKANQA__REDIS_CONN_ID')

        if import_from_redis:
            self._check_key_redis()
        else:  # Set/Overwrite on redis via @property.setter
            self.ckan_api_base_url = ckan_api_base_url
            self.connector_class = connector_class  # Currently only MinioConnector supported
            self.checkpoint_success = []
            self.airflow_connection_id = v if (v := airflow_connection_id) else Variable.get('CKANQA__S3_CONN_ID')
            self.data_bucket_name = v if (v := data_bucket_name) else Variable.get('CKANQA__S3_BUCKET_NAME_DATA')

    @classmethod
    def generate_context_from_airflow_execute(
        cls,
        operator: CkanBaseOperator,
        airflow_context,
        import_from_redis: bool = False
    ):
        return cls(
            operator.ckan_name,
            operator.get_dag_runtime(airflow_context),
            import_from_redis=import_from_redis
        )

    @property
    def dag_runtime_iso_8601_basic(self):
        format = Variable.get('CKANQA__STRFTIME_FORMAT')
        return self.dag_runtime.strftime(format)

    @property
    def dag_runtime(self) -> dt.datetime:
        if self._dag_runtime:
            return self._dag_runtime
        else:
            raise ValueError('Currently, context`s dag_runtime is not set.')

    @dag_runtime.setter
    def dag_runtime(self, value: dt.datetime) -> None:
        self._dag_runtime = value

    @property
    def ckan_metadata(self):
        REDIS_PROPERTY = 'ckan_metadata'
        try:
            response = self._redis_get(REDIS_PROPERTY)
            logging.debug('Found cached response in redis, load from there.')

        # When no metadata stored in Redis, load from CKAN
        except KeyError:
            logging.debug('Not found cached response in redis, load from CKAN API.')
            hook = CkanInstanceHook(self.ckan_api_base_url)
            response = hook.get_metadata(self.ckan_name)
            self._redis_set(REDIS_PROPERTY, response)
        return json.loads(response)

    @property
    def download_urls(self) -> List[str]:
        """Extracts download URLs for the selected CKAN package."""
        return [i['download_url'] for i in self.ckan_metadata['result']['resources']]

    @property
    def ckan_org_name(self) -> str:
        return self.ckan_metadata['result']['organization']['name']

    @property
    def ckan_id(self) -> str:
        return self.ckan_metadata['result']['id']

    @property
    def ckan_context_name(self) -> str:
        return '@'.join([self.ckan_name, self.ckan_org_name])

    @property
    def default_checkpoint_name(self):
        return '@'.join(['default_checkpoint', self.ckan_context_name])

    @property
    def default_datasource_name(self):
        return '@'.join(['datasource', self.ckan_context_name])

    @property
    def default_data_connector_name(self):
        return '@'.join(['data_connector', self.ckan_context_name])

    @property
    def default_s3_data_prefix(self):
        return '/'.join([self.ckan_name, self.dag_runtime_iso_8601_basic])

    @property
    def ckan_api_base_url(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @ckan_api_base_url.setter
    def ckan_api_base_url(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def data_bucket_name(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @data_bucket_name.setter
    def data_bucket_name(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def checkpoint_success(self) -> List[bool]:
        response = self._redis_get(inspect.stack()[0][3])
        return json.loads(response)

    @checkpoint_success.setter
    def checkpoint_success(self, value: List[bool]) -> None:
        json_value = json.dumps(value)
        self._redis_set(inspect.stack()[0][3], json_value, ttl=None)

    @property
    def connector_class(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @connector_class.setter
    def connector_class(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def airflow_connection_id(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @airflow_connection_id.setter
    def airflow_connection_id(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def boto3_options(self) -> dict:
        credentials = BaseHook.get_connection(conn_id=self.airflow_connection_id)
        boto3_options = {
            'endpoint_url': json.loads(credentials.get_extra())['endpoint_url'],
            'aws_access_key_id': credentials.login,
            'aws_secret_access_key': credentials._password,
        }
        return boto3_options

    def set_runtime_from_context(self, airflow_context) -> CkanContext:
        """Sets _dag_runtime from airflow context."""
        self.dag_runtime = airflow_context.get('dag_run').execution_date
        return self

    def _check_key_redis(self) -> None:
        """Checks, whether all the properties are also stored in Redis."""
        # IF import is set, check, whether all keys are available on Redis
        # Skip the first two properties, because they are part of the key
        try:
            for property in self.PERSISTED_PROPERTIES[2:]:
                getattr(self, property)
        except KeyError:
            raise Exception('Not all expected keys from CkanContext are found on Redis. Cannot import.')

    def build_valiation_checkpoint_name(self, validation_name: str):
        return '@'.join([f'validation_{validation_name}', self.ckan_context_name])

    def _build_key(self, property: str):
        """
        Example key:
            'ckan:context:aktualisierte-luftqualitatsmessungen-seit-1983:20220211T000000.000000Z:selected_checkpoint'
            'ckan:checkpoint:luftqualitatsmessungen:20220211T000000.000000Z:defaultcheckpoint:connector_class'

        """
        return ':'.join(filter(None, [
            self.REDIS_KEY_PARTS[0],
            self.REDIS_KEY_PARTS[1],
            self.ckan_name,
            self.dag_runtime_iso_8601_basic,
            getattr(self, 'validation_name', None),  # Only set inside CheckpointContext
            property
        ]))

    def _redis_get(self, property: str) -> str:
        hook = RedisHook(self.REDIS_CONN_ID)
        with hook.get_conn() as conn:
            key = self._build_key(property)
            if not conn.exists(key):
                raise KeyError(f'Redis key {key} not found.')
            response = conn.get(key)
            if response is None:
                raise ValueError(f'Error with get key {key} from Redis.')
            logging.debug(f'REDIS READ: key {key}')
            return response.decode('utf-8')

    def _redis_set(self, property: str, value: str, ttl: int | None = None) -> None:
        """When ttl = None, then no expiration."""
        if ttl is None:
            ttl = Variable.get('CKANQA__REDIS_DEFAULT_TTL')
        hook = RedisHook(self.REDIS_CONN_ID)
        with hook.get_conn() as conn:
            key = self._build_key(property)
            if conn.exists(key):
                logging.debug(f'Key {key} already existent on Redis. Overwrite.')
            if not isinstance(value, str):
                raise ValueError(f'Insert value for Redis is of type {type(value)} instead of str.')
            conn.set(key, value, ex=ttl)
            logging.debug(f'REDIS WRITE: key {key}, value {value[:20]}, ttl {ttl}')

    def _redis_append_with_lock(self, property: str, value: Any) -> None:
        """Append one element to a list, with db lock."""
        hook = RedisHook(self.REDIS_CONN_ID)
        with hook.get_conn() as conn:
            with conn.lock('lock'):
                response = json.loads(self._redis_get(property))
                response.append(value)
                modified_response = json.dumps(response)
                self._redis_set(property, modified_response)

    def _redis_delete(self, property: str) -> None:
        hook = RedisHook(self.REDIS_CONN_ID)
        key = self._build_key(property)
        with hook.get_conn() as conn:
            if not conn.exists(key):
                raise KeyError(f'Redis key {key} not found.')
            conn.delete(key)
            logging.debug(f'REDIS DELETE: key {key}')

    def delete_context_redis(self):
        """Deletes all persisted properties from redis."""
        for property in self.PERSISTED_PROPERTIES[2:]:
            self._redis_delete(property)

    def append_checkpoint_success(self, value: bool) -> None:
        """Append boolean to checkpoint_success to later track status of checkpoint execution."""
        self._redis_append_with_lock('checkpoint_success', value)
