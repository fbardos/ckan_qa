import importlib
import io
import logging
import os
from typing import Generator, Literal, Tuple, Union

import pandas as pd
import requests

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.data_context.abstract_data_context import \
    AbstractDataContext
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig, DataContextConfig, S3StoreBackendDefaults)
from great_expectations.exceptions import DataContextError

from ckanqa.connector import FilesystemBaseConnector, MinioConnector

# According to GE Docs, when using an experimental expectation, an import is needed
# when Expectation Suite is created AND when checkpoint is defined and run.
from great_expectations_experimental.expectations.expect_column_values_to_change_between import \
    ExpectColumnValuesToChangeBetween


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
        try:
            response = requests.get(url, params={'id': package_name})
        except requests.exceptions.RequestException as err:
            raise SystemExit(err)
        return response.text


class CkanFilesystemHook(BaseHook):

    def __init__(
        self,
        connection_id: str | None = None,
        connector_class: Literal['MinioConnector', 'SftpConnector'] = 'MinioConnector',
        **kwargs
    ):
        self.connection_id = v if (v := connection_id) else Variable.get('CKANQA__S3_CONN_ID')

        # Load connector class
        mod = importlib.import_module('ckanqa.connector')
        self.connector: FilesystemBaseConnector = getattr(mod, connector_class)(**kwargs)


class CkanDataHook(CkanFilesystemHook):
    """Connects to data, e.g. on Minio.

    Call structure, per default:
    CkanDataHook
        --> calls MinioConnector
            --> calls S3Hook (airflow.providers.amazon)

    Bucket name for S3/Minio gets stored inside connector and should not be part of this
    generic CkanDataHook class because other connetors can also by served.
    If MinioConnector is selected as connector, pass bucket_name to via kwargs.

    """

    def _build_file_path(self, ckan_name: str, iso_8601_basic: str, filename: str | None = None) -> str:
        return '/'.join(filter(None, [ckan_name, iso_8601_basic, filename]))

    def write_buffer_to_target(self, ckan_name: str, iso_8601_basic: str, filename: str, buffer: io.BytesIO):
        """ISO 8601 BASIC is needed as directory name."""
        key = self._build_file_path(ckan_name, iso_8601_basic, filename)
        self.connector.write_to_target(key, buffer)

    def load_from_directory(self, ckan_name: str, iso_8601_basic: str) -> Generator[Tuple[str, io.BytesIO], None, None]:
        """Loads all files files from directory."""
        prefix = self._build_file_path(ckan_name, iso_8601_basic)
        for key, buffer in self.connector.load_directory_from_target(prefix):
            yield key, buffer

    def write_from_request(self, ckan_name: str, iso_8601_basic: str, filename: str, response: requests.Response):
        prefix = self._build_file_path(ckan_name, iso_8601_basic, filename)
        self.connector.write_to_target_request_content(prefix, response)

    def write_from_buffer(self, ckan_name: str, iso_8601_basic: str, filename: str, buffer: io.BytesIO):
        prefix = self._build_file_path(ckan_name, iso_8601_basic, filename)
        self.connector.write_to_target(prefix, buffer)

    def load_dataframes_from_ckan(
        self, ckan_name: str, iso_8601_basic: str, filetype: Literal['.csv', '.parquet'] = '.csv'
    ) -> Generator[Tuple[str, pd.DataFrame], None, None]:
        """ Returns generator """
        prefix = self._build_file_path(ckan_name, iso_8601_basic)
        for filepath in self.connector.list_path_objects(prefix):
            if os.path.splitext(filepath)[1] == filetype:  # match file ending
                yield filepath, self.connector.load_dataframe_from_target(filepath)
            else:
                continue

    def delete_files(self, keys: Union[str, list]):
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            self.connector.delete_from_target(key)


class GreatExpectationsHook(CkanFilesystemHook):
    """Connects to GE on Minio."""
    DATA_DOCS_SITE_NAME = 's3_site'

    def get_base_data_context(self) -> AbstractDataContext:
        bucket_name_meta = Variable.get('CKANQA__S3_BUCKET_NAME_META')

        # Only supported for MinioConnector
        assert isinstance(self.connector, MinioConnector)

        # Build GreatExpectations data context
        config_data_docs_sites = {
            self.DATA_DOCS_SITE_NAME: {
                'class_name': 'SiteBuilder',
                'show_how_to_buttons': False,
                'store_backend': {
                    'class_name': 'TupleS3StoreBackend',
                    'bucket': bucket_name_meta,
                    'prefix': 'data_docs',
                    'boto3_options': self.connector.boto3_options,
                },
            },
        }
        data_context_config = DataContextConfig(
            store_backend_defaults=S3StoreBackendDefaults(
                default_bucket_name=bucket_name_meta,
            ),
            data_docs_sites=config_data_docs_sites,
            anonymous_usage_statistics=AnonymizedUsageStatisticsConfig(enabled=False)
        )

        # Rewrite store_backend_defaults config
        for store in data_context_config.stores.values():
            if store.get('store_backend', {}).get('class_name') == 'TupleS3StoreBackend':
                store['store_backend']['boto3_options'] = self.connector.boto3_options

        # Return GE context
        return BaseDataContext(project_config=data_context_config)

    def get_suite(self, suite_name: str):

        context = self.get_base_data_context()
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

    """
    Currently not needed, but when reenabled, it should be inserted as separate Airflow Operator.

    Currently, all the datasource_config gets built inside GeExecuteCheckpointOperator.
    Maybe, there is a simpler way, without defining a Checkpoint before?
    """
    # def add_pandas_datasource(self, name: str):
    #     datasource_config = {
    #         'name': name,
    #         'class_name': 'Datasource',
    #         'module_name': 'great_expectations.datasource',
    #         'execution_engine': {
    #             'module_name': 'great_expectations.execution_engine',
    #             'class_name': 'PandasExecutionEngine',
    #         },
    #         'data_connectors': {
    #             'default_runtime_data_connector_name': {
    #                 'class_name': 'RuntimeDataConnector',
    #                 'module_name': 'great_expectations.datasource.data_connector',
    #                 'batch_identifiers': ['batch_id'],
    #             },
    #         },
    #     }
    #     context = self.get_base_data_context()
    #     context.test_yaml_config(yaml.dump(datasource_config))
    #     datasource = context.add_datasource(**datasource_config)
    #     return datasource

    def add_expectation(self, suite_name: str, expectation_config: ExpectationConfiguration):
        """
        """
        context = self.get_base_data_context()
        suite = self.get_suite(suite_name)
        expectation = suite.add_expectation(expectation_configuration=expectation_config)
        context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=suite_name)
        return expectation

    def add_or_update_checkpoint(self, checkpoint_name: str, suite_name: str, **kwargs):
        context = self.get_base_data_context()
        checkpoint = context.add_or_update_checkpoint(
            name=checkpoint_name,
            config_version=1,
            class_name='Checkpoint',
            expectation_suite_name=suite_name,
            **kwargs
        )
        return checkpoint
