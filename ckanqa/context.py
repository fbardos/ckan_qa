import datetime as dt
import inspect
import json
import logging
from typing import Optional, Union

from airflow.providers.redis.hooks.redis import RedisHook
from ckanqa.constant import (DEFAULT_REDIS_CONN_ID, DEFAULT_S3_CONN_ID,
                             ISO8601_BASIC_FORMAT, REDIS_DEFAULT_TTL,
                             S3_BUCKET_NAME_DATA)
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
    PERSISTED_PROPERTIES = [
        'ckan_name',
        'dag_runtime',
        'ckan_api_base_url',
        'connector_class',
        'airflow_connection_id',
        'data_bucket_name',
        'data_connector_name',
        'datasource_name',
        'batch_request_data_asset_name',
        'boto3_options',
        'selected_checkpoint',
        'checkpoint_configs',
        'checkpoint_success',
    ]
    # TODO: Are ther other GE configs, which can be made obsolete, because consistently persisted to S3?

    # One context is uniquely identified by ckan_name and dag_runtime
    def __init__(
        self,
        ckan_name: str,
        dag_runtime: dt.datetime = dt.datetime(1970, 1, 1),
        import_from_redis: bool = False,
        ckan_api_base_url: str = 'https://ckan.opendata.swiss',
        connector_class: str = 'MinioConnector',
        airflow_connection_id: str = DEFAULT_S3_CONN_ID,
        data_bucket_name: str = S3_BUCKET_NAME_DATA,
        data_connector_name: str = 'default_runtime_data_connector_name'
    ):

        # General, part of REDIS keys
        self.ckan_name = ckan_name
        self.dag_runtime = dag_runtime

        # IF import is set, check, whether all keys are available on Redis
        # Skip the first two properties, because they are part of the key
        if import_from_redis:
            try:
                for property in self.PERSISTED_PROPERTIES[2:]:
                    getattr(self, property)
            except KeyError:
                raise Exception('Not all expected keys from CkanContext are found on Redis. Cannot import.')

        # ELSE set according to __input__ parameters
        else:

            # __init__ input parameters, persisted to REDIS
            self.ckan_api_base_url = ckan_api_base_url
            self.connector_class = connector_class  # Currently only MinioConnector supported
            self.airflow_connection_id = airflow_connection_id
            self.data_bucket_name = data_bucket_name
            self.data_connector_name = data_connector_name  # For GE

            # Other properties, persisted to REDIS
            self.datasource_name = ''
            self.batch_request_data_asset_name = ''
            self.boto3_options = {}
            self.selected_checkpoint = ''  # Maybe find a better solution...
            self.checkpoint_configs = {}  # Can contain multiple checkpoint_configs
            self.checkpoint_success = None

    @classmethod
    def generate_context_from_airflow_execute(cls, operator: CkanBaseOperator, airflow_context, import_from_redis: bool = False):
        return cls(
            operator.ckan_name,
            operator.get_dag_runtime(airflow_context),
            import_from_redis=import_from_redis
        )

    @property
    def dag_runtime_iso_8601_basic(self):
        return self.dag_runtime.strftime(ISO8601_BASIC_FORMAT)

    def _build_key(self, property: str):
        return ':'.join(['ckan', self.ckan_name, self.dag_runtime_iso_8601_basic, property])

    def _redis_get(self, property: str) -> str:
        hook = RedisHook(DEFAULT_REDIS_CONN_ID)
        with hook.get_conn() as conn:
            key = self._build_key(property)
            if not conn.exists(key):
                raise KeyError(f'Redis key {key} not found.')
            response = conn.get(key)
            if response is None:
                raise ValueError(f'Error with get key {key} from Redis.')
            logging.debug(f'REDIS READ: key {key}')
            return response.decode('utf-8')

    def _redis_set(self, property: str, value: str, ttl: Optional[int] = REDIS_DEFAULT_TTL) -> None:
        """When ttl = None, then no expiration."""
        hook = RedisHook(DEFAULT_REDIS_CONN_ID)
        with hook.get_conn() as conn:
            key = self._build_key(property)
            if conn.exists(key):
                logging.debug(f'Key {key} already existent on Redis. Overwrite.')
            if not isinstance(value, str):
                raise ValueError(f'Insert value for Redis is of type {type(value)} instead of str.')
            conn.set(key, value, ex=ttl)
            logging.debug(f'REDIS WRITE: key {key}, value {value[:20]}, ttl {ttl}')

    def _redis_delete(self, property: str) -> None:
        hook = RedisHook(DEFAULT_REDIS_CONN_ID)
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
    def ckan_org_name(self) -> str:
        return self.ckan_metadata['result']['organization']['name']

    @property
    def ckan_id(self) -> str:
        return self.ckan_metadata['result']['id']

    @property
    def suite_name(self) -> str:
        return '@'.join([self.ckan_name, self.ckan_org_name])

    @property
    def default_checkpoint_name(self):
        return '@'.join(['default_checkpoint', self.suite_name])

    @property
    def default_datasource_name(self):
        return '@'.join(['datasource', self.suite_name])

    # Currently not needed, is set always on GeBatchRequestOnS3Operator
    # @property
    # def default_data_asset_name(self):
        # return '@'.join(['default_data_asset_name', self.suite_name])

    @property
    def ckan_api_base_url(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @ckan_api_base_url.setter
    def ckan_api_base_url(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

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
    def data_bucket_name(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @data_bucket_name.setter
    def data_bucket_name(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def data_connector_name(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @data_connector_name.setter
    def data_connector_name(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def datasource_name(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @datasource_name.setter
    def datasource_name(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def batch_request_data_asset_name(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @batch_request_data_asset_name.setter
    def batch_request_data_asset_name(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def boto3_options(self) -> dict:
        response = self._redis_get(inspect.stack()[0][3])
        return json.loads(response)

    @boto3_options.setter
    def boto3_options(self, value: dict) -> None:
        json_value = json.dumps(value)
        self._redis_set(inspect.stack()[0][3], json_value, ttl=None)

    @property
    def selected_checkpoint(self) -> str:
        return self._redis_get(inspect.stack()[0][3])

    @selected_checkpoint.setter
    def selected_checkpoint(self, value: str) -> None:
        self._redis_set(inspect.stack()[0][3], value, ttl=None)

    @property
    def checkpoint_configs(self) -> dict:
        response = self._redis_get(inspect.stack()[0][3])
        return json.loads(response)

    @checkpoint_configs.setter
    def checkpoint_configs(self, value: dict) -> None:
        json_value = json.dumps(value)
        self._redis_set(inspect.stack()[0][3], json_value, ttl=None)

    @property
    def checkpoint_success(self) -> Union[bool, None]:
        response = self._redis_get(inspect.stack()[0][3])
        if response == '-1':
            return None
        else:
            return bool(int(response))

    @checkpoint_success.setter
    def checkpoint_success(self, value: Optional[bool] = None) -> None:
        if value is None:
            bool_value = '-1'
        else:
            bool_value = str(int(value))
        self._redis_set(inspect.stack()[0][3], bool_value, ttl=None)

    def attach_empty_checkpoint_config(self, checkpoint_name: Optional[str] = None) -> None:
        if checkpoint_name is None:
            checkpoint_name = self.selected_checkpoint
        configs = self.checkpoint_configs
        configs[checkpoint_name] = {
            'checkpoint_config': {},
            'suite': {
                'suite_config': {},  # Not needed atm? Is generate during GeCheckSuiteOperator
                # Is disable to parallelize expectation creation.
                # 'expectations': [],
            },
            'batch_request': {
                'batch_request_config': {},
                'datasource_config': {},
            }
        }
        self.checkpoint_configs = configs

    def add_suite_config(self, suite_config: dict, checkpoint_name: Optional[str] = None) -> None:
        if checkpoint_name is None:
            checkpoint_name = self.selected_checkpoint
        config = self.checkpoint_configs
        config[checkpoint_name]['suite']['suite_config'] = suite_config
        self.checkpoint_configs = config

    def add_batch_request_config(self, batch_request_config: dict, checkpoint_name: Optional[str] = None) -> None:
        if checkpoint_name is None:
            checkpoint_name = self.selected_checkpoint
        config = self.checkpoint_configs
        config[checkpoint_name]['batch_request']['batch_request_config'] = batch_request_config
        self.checkpoint_configs = config

    def add_datasource_config(self, datasource_config: dict, checkpoint_name: Optional[str] = None) -> None:
        if checkpoint_name is None:
            checkpoint_name = self.selected_checkpoint
        config = self.checkpoint_configs
        config[checkpoint_name]['batch_request']['datasource_config'] = datasource_config
        self.checkpoint_configs = config

    def add_checkpoint_config(self, checkpoint_config: dict, checkpoint_name: Optional[str] = None) -> None:
        if checkpoint_name is None:
            checkpoint_name = self.selected_checkpoint
        config = self.checkpoint_configs
        config[checkpoint_name]['checkpoint_config'] = checkpoint_config
        self.checkpoint_configs = config
