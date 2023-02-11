import io
import json
import logging
from typing import Generator, Optional, Tuple, Union

import pandas as pd
import requests
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig, DataContextConfig, S3StoreBackendDefaults)
from great_expectations.exceptions import DataContextError
from markdown import Markdown
from nio import AsyncClient
from nio.responses import RoomResolveAliasError
from ruamel import yaml

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from ckanqa.connectors import MinioConnector
from ckanqa.constant import (DEFAULT_S3_CONN_ID, S3_BUCKET_NAME_DATA,
                             S3_BUCKET_NAME_META)


class CkanInstanceHook(BaseHook):
    """Connects to CKAN instance, e.g. opendata.swiss"""
    CKAN_API_SUBPATH = 'api/3/action'

    def __init__(self, base_url: str = 'https://ckan.opendata.swiss'):
        self.base_url = base_url

    @property
    def ckan_api_url(self):
        return '/'.join([self.base_url, CkanInstanceHook.CKAN_API_SUBPATH])

    def get_endpoint_url(self, endpoint: str):
        return '/'.join([self.ckan_api_url, endpoint])

    def get_metadata(self, package_name: str):
        ENDPOINT = 'package_show'
        url = self.get_endpoint_url(ENDPOINT)
        response = requests.get(url, params={'id': package_name})
        # TODO: Add Error handling for HTTP-Codes != 200
        return response.text


class CkanDataHook(BaseHook):
    """Connects to data, e.g. on Minio.

    Call structure, per default:
    CkanDataHook
        --> calls MinioConnector
            --> calls S3Hook (airflow.providers.amazon)

    """
    # TODO: Use this Hook for all kind of operations with data (not only delete)

    def __init__(self, connection_id: str = DEFAULT_S3_CONN_ID):
        self.connection_id = connection_id
        self.connector = MinioConnector(connection_id)
        self.bucket = S3_BUCKET_NAME_DATA

    def list_files(self, path: str):
        return self.connector.list_files_paths(path)

    def write_from_request(self, ckan_name: str, iso_8601_basic: str, filename: str, response: requests.Response):
        self.connector.write_to_source_request(ckan_name, iso_8601_basic, filename, response)

    def write_from_buffer(self, ckan_name: str, iso_8601_basic: str, filename: str, buffer: io.BytesIO):
        self.connector.write_to_source_buffer(ckan_name, iso_8601_basic, filename, buffer)

    def load_dataframe_from_filetype(
        self, ckan_name: str, iso_8601_basic: str, filetype: str = '.csv'
    ) -> Generator[Tuple[str, pd.DataFrame], None, None]:
        """

        Returns:
            Iterator

        """
        for file, df in self.connector.load_dataframe_from_filetype(ckan_name, iso_8601_basic, filetype):
            yield file, df

    def delete_files(self, keys: Union[str, list]):
        self.connector.delete_from_source(keys=keys)


class GreatExpectationsHook(BaseHook):
    """Connects to GE on Minio."""
    # TODO: Should return GE context from S3-stored GE
    DATA_DOCS_SITE_NAME = 's3_site'

    def __init__(self, connection_id: str = DEFAULT_S3_CONN_ID):
        self.connection_id = connection_id
        self.connector = MinioConnector(connection_id)
        self.bucket = S3_BUCKET_NAME_META

    def get_context(self):

        # Load credentails from BaseHook
        # TODO: Better would be to use as custom S3 Hook instead.
        credentials = BaseHook.get_connection(conn_id=self.connection_id)

        boto3_options = {
            'endpoint_url': json.loads(credentials.get_extra())['endpoint_url'],
            'aws_access_key_id': credentials.login,
            'aws_secret_access_key': credentials._password,
        }

        # Build GreatExpectations data context
        config_data_docs_sites = {
            self.DATA_DOCS_SITE_NAME: {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": S3_BUCKET_NAME_META,
                    "prefix": "data_docs",
                    "boto3_options": boto3_options,
                },
            },
        }
        data_context_config = DataContextConfig(
            store_backend_defaults=S3StoreBackendDefaults(
                default_bucket_name=S3_BUCKET_NAME_META
            ),
            data_docs_sites=config_data_docs_sites,
            anonymous_usage_statistics=AnonymizedUsageStatisticsConfig(enabled=False)
        )

        # Rewrite store_backend_defaults config
        for store in data_context_config['stores'].values():
            if store.get('store_backend', {}).get('class_name') == 'TupleS3StoreBackend':
                store['store_backend']['boto3_options'] = boto3_options

        # Return GE context
        return BaseDataContext(project_config=data_context_config)

    def get_suite(self, suite_name: str):
        context = self.get_context()
        try:
            suite = context.get_expectation_suite(expectation_suite_name=suite_name)
            logging.info(
                f'Loaded ExpectationSuite "{suite.expectation_suite_name}", '
                f'containing {len(suite.expectations)} expectations.'
            )

        # If not already present create a new suite.
        except DataContextError:
            suite = context.create_expectation_suite(expectation_suite_name=suite_name)
            logging.info(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

        return suite

    def add_pandas_datasource(self, name: str):
        datasource_config = {
            'name': name,
            'class_name': 'Datasource',
            'module_name': 'great_expectations.datasource',
            'execution_engine': {
                'module_name': 'great_expectations.execution_engine',
                'class_name': 'PandasExecutionEngine',
            },
            'data_connectors': {
                'default_runtime_data_connector_name': {
                    'class_name': 'RuntimeDataConnector',
                    'module_name': 'great_expectations.datasource.data_connector',
                    'batch_identifiers': ['batch_id'],
                },
            },
        }
        context = self.get_context()
        context.test_yaml_config(yaml.dump(datasource_config))
        datasource = context.add_datasource(**datasource_config)
        return datasource

    def add_pandas_batch_request(self, datasource_name: str, data_asset_name: str, df: pd.DataFrame):
        batch_request = RuntimeBatchRequest(
            datasource_name=datasource_name,
            data_connector_name="default_runtime_data_connector_name",  # TODO: Is this to be renamed?
            data_asset_name=data_asset_name,
            batch_identifiers={"batch_id": "default_identifier"},
            runtime_parameters={"batch_data": df},
        )
        return batch_request

    def add_expectation(self, suite_name: str, expectation_type: str, meta: Optional[dict] = None, **kwargs):
        """

        Args:
            suite_name: Name of the corresponding suite.
            expectation_type: Name of expectation type being added,
                e.g expect_table_columns_to_match_ordered_list
            meta: Meta information, like:
                meta={
                   "notes": {
                      "format": "markdown",
                      "content": "Some clever comment about this expectation. **Markdown** `Supported`"
                   }
                }



        """
        context = self.get_context()
        suite = self.get_suite(suite_name)
        expectation_configuration = ExpectationConfiguration(
            expectation_type=expectation_type,
            kwargs=kwargs,
            meta=meta,
        )
        expectation = suite.add_expectation(expectation_configuration=expectation_configuration)
        context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=suite_name)
        return expectation

    def add_checkpoint(self, checkpoint_name: str, suite_name: str):
        context = self.get_context()
        checkpoint = context.add_checkpoint(
            name=checkpoint_name,
            config_version=1,
            class_name='SimpleCheckpoint',
            expectation_suite_name=suite_name,
            site_names=[self.DATA_DOCS_SITE_NAME],  # Not needed... per default will update all data_docs_sites
        )
        return checkpoint

    # Currently not needed?
    # def run_checkpoint(
        # self,
        # checkpoint_name: str,
        # suite_name: str,
        # datasource_name: str,
        # data_asset_name:str,
        # df: pd.DataFrame  # TODO: not always needed with injected dataframe
    # ):
        # context = self.get_context()
        # result = context.run_checkpoint(
            # checkpoint_name=checkpoint_name,
            # expectation_suite_name=suite_name,
            # batch_request=self.add_pandas_batch_request(datasource_name, data_asset_name, df),
        # )
        # return result


class MatrixHook(BaseHook):

    def __init__(self, conn_id: str):
        super().__init__()
        self.conn = self.get_connection(conn_id)
        if isinstance(self.conn.host, str) and isinstance(self.conn.login, str):
            pass
        else:
            raise ValueError('Stored host and login must be set.')
        self.client = AsyncClient(self.conn.host, self.conn.login)

    async def _login(self):
        """Returns token..."""
        r = await self.client.login(self.conn.password)
        logging.info('Logged in to matrix homeserver.')
        return r

    async def _close(self):
        r = await self.client.close()
        logging.info('Logged out of matrix homeserver')
        return r

    async def resolve_alias(self, alias: str):
        await self._login()
        try:
            r = await self.client.room_resolve_alias(alias)
            if isinstance(r, RoomResolveAliasError):  # When room does not exist, create one
                raise AirflowException(f'Room alias {alias} could not be resolved.')
        finally:
            await self._close()
        return r.room_id

    async def send_markdown_message(self, room_id: str, message: str):
        """Sends message to matrix server.

        This method needs error handling, e.g. when sent message is too big.

        """
        _ = await self._login()
        try:
            msg_html = Markdown().convert('<pre><code>' + message + '</code></pre>')
            _ = await self.client.room_send(
                room_id=room_id,
                message_type='m.room.message',
                content={
                    "msgtype": "m.text",
                    "format": "org.matrix.custom.html",
                    "body": message,
                    "formatted_body": msg_html,
                }
            )
        finally:
            _ = await self._close()
