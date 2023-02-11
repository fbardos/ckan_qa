"""
Contains Operators for Airflow.
"""
import json
import logging
from typing import Dict, List, Optional

from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration
from ruamel import yaml

from airflow.hooks.base import BaseHook
from ckanqa.constant import DEFAULT_S3_CONN_ID
from ckanqa.context import CkanContext
from ckanqa.hook import GreatExpectationsHook
from ckanqa.operator.ckan import CkanBaseOperator


class CkanContextSetter(CkanBaseOperator):

    def __init__(self, ckan_name: str, **kwargs):
        super().__init__(ckan_name=ckan_name, **kwargs)

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=False)
        ckan_context.datasource_name = ckan_context.default_datasource_name
        ckan_context.selected_checkpoint = ckan_context.default_checkpoint_name
        ckan_context.attach_empty_checkpoint_config()


class GeCheckSuiteOperator(CkanBaseOperator):
    """ Checks, if the desired suite is present.

    Otherwise, creates it.
    Name of the suite is automatically generated with the following syntax:
        org-name/dataset-name

    """

    def __init__(self, ckan_name: str, **kwargs):
        super().__init__(ckan_name=ckan_name, **kwargs)

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        hook = GreatExpectationsHook(ckan_context.airflow_connection_id)
        hook.get_suite(ckan_context.suite_name)


class GeRemoveExpectationsOperator(CkanBaseOperator):
    """ Removes existing expectation, for later recreation.

    Args:
        - expectation_configuration: If not set, every expectation gets deleted.

    """

    def __init__(
        self,
        ckan_name: str,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        remove_multiple_matches: bool = True,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.remove_multiple_matches = remove_multiple_matches
        if expectation_configuration:
            self.expectation_configuration = expectation_configuration
        else:

            # When no expectation_configuration is given, all expectations
            # will be deleted.
            self.expectation_configuration = {}

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        hook = GreatExpectationsHook(ckan_context.airflow_connection_id)
        ge_context = hook.get_context()
        suite = hook.get_suite(ckan_context.suite_name)
        logging.debug(f'Currently saved expectations: {suite.expectations}')
        if len(suite.expectations) == 0:
            logging.warning('There were no saved expectations. Skip.')
        else:
            suite.remove_expectation(
                expectation_configuration=self.expectation_configuration,
                remove_multiple_matches=self.remove_multiple_matches,
            )
            ge_context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=self.suite_name)


class GeBatchRequestOnS3Operator(CkanBaseOperator):
    """Register GE datasource from S3 using pandas.


    Alternative would be:
        - to load datasource as pandas dataframe during DAG run.
        - to load with Spark instead of pandas.

    """

    def __init__(
        self,
        ckan_name: str,
        batch_request_data_asset_name: str,
        datasource_name: Optional[str] = None,
        regex_filter: str = r'.*',
        regex_filter_groups: List['str'] = ['data_asset_name'],
        prefix: Optional[str] = None,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.batch_request_data_asset_name = batch_request_data_asset_name
        self.datasource_name = datasource_name
        self.regex_filter = regex_filter
        self.regex_filter_groups = regex_filter_groups
        self.prefix = prefix

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)

        # If datasource_name is set, overwrite
        if self.datasource_name:
            ckan_context.datasource_name = self.datasource_name

        # If prefix == None, then generate according to suspected path:
        #   ckan_name/iso_date
        if not self.prefix:
            # TODO: DRY: Prefix-generation is defined on multiple locations
            dag_run_timestamp = ckan_context.dag_runtime_iso_8601_basic
            self.prefix = '/'.join([self.ckan_name, dag_run_timestamp])

        # Generate boto3_options
        # TODO: DRY: Already used in ge_hook.GreatExpectationsHook
        # TODO: Better would be to use as custom S3 Hook instead.
        credentials = BaseHook.get_connection(conn_id=ckan_context.airflow_connection_id)
        boto3_options = {
            'endpoint_url': json.loads(credentials.get_extra())['endpoint_url'],
            'aws_access_key_id': credentials.login,
            'aws_secret_access_key': credentials._password,
        }
        ckan_context.boto3_options = boto3_options

        # Generate GE datasource config
        bucket = ckan_context.data_bucket_name
        # batch_identifier_name = 'parquet_batch'  # TODO: Add to context? --> currently not needed
        datasource_config = {
            'name': ckan_context.datasource_name,
            'class_name': 'Datasource',
            'execution_engine': {
                'class_name': 'PandasExecutionEngine',
                'boto3_options': boto3_options,  # This needs to set manually
            },
            'data_connectors': {
                # data_connector_name: {
                    # 'class_name': 'RuntimeDataConnector',
                    # 'batch_identifiers': [batch_identifier_name],
                # },
                ckan_context.data_connector_name: {
                    'class_name': 'InferredAssetS3DataConnector',
                    # TODO: What about proto3 options??
                    'bucket': bucket if not bucket.endswith('/') else bucket + '/',
                    'prefix': self.prefix if not self.prefix.endswith('/') else self.prefix + '/',
                    'boto3_options': boto3_options,
                    'default_regex': {
                        'pattern': self.regex_filter,
                        # You can assign group names to regex groups here.
                        # With this regex, you can assign data_asset_name
                        # to a group of the regex search.
                        'group_names': self.regex_filter_groups,
                    },
                },
            },
        }
        ckan_context.add_datasource_config(datasource_config)

        # Validate config
        hook = GreatExpectationsHook(ckan_context.airflow_connection_id)
        ge_context = hook.get_context()
        suite = hook.get_suite(ckan_context.suite_name)
        ge_context.test_yaml_config(yaml.dump(datasource_config))

        # Save datasource
        datasource = ge_context.add_datasource(**datasource_config, save_changes=True)

        # Print information about the data assets
        logging.info('datasource self-check', datasource.self_check())


        # TODO: I think, this is not needed. Gets rewritten by validator.save_expectation_suite()
        # ge_context.save_expectation_suite(suite, expectation_suite_name=self.suite_name)

        # Create BatchRequest (RuntimeBatchRequest is only needed when passing a in-memory dataframe to validator).
        # TODO: Add additional operator for RuntimeBatchRequest...
        batch_request_config = dict(
            datasource_name=ckan_context.datasource_name,
            data_connector_name=ckan_context.data_connector_name,
            data_asset_name=self.batch_request_data_asset_name,
        )
        batch_request = BatchRequest(**batch_request_config)
        ckan_context.add_batch_request_config(batch_request_config)

        validator = ge_context.get_validator(
            batch_request=batch_request, expectation_suite_name=ckan_context.suite_name
        )
        validator.save_expectation_suite(discard_failed_expectations=False)


class GeBuildExpectationOperator(CkanBaseOperator):
    """Register Ge datasource via pandas DataFrame.

    Suite name gets automatically generated.

    """
    def __init__(
        self,
        ckan_name: str,
        expectation_type: str,
        ge_kwargs: Dict,
        ge_meta: Optional[Dict] = None,
        connection_id: str = DEFAULT_S3_CONN_ID,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.expectation_type = expectation_type
        self.ge_meta = ge_meta
        self.ge_kwargs = ge_kwargs
        self.connection_id = connection_id

    def execute(self, context):
        """

        Does not save any config to ckan_context, otherwise, not parallelization
        would be possible.

        """
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)

        hook = GreatExpectationsHook(self.connection_id)
        suite = hook.get_suite(ckan_context.suite_name)
        expectation_config = dict(
            suite_name=ckan_context.suite_name,
            expectation_type=self.expectation_type,
            meta=self.ge_meta,
            **self.ge_kwargs
        )

        # No suite saving requred. Gets automatically persisted.
        _ = hook.add_expectation(**expectation_config)
        # self.ckan_context.append_expectation_config(expectation_config)


class GeBuildCheckpointOperator(CkanBaseOperator):
    """A Checkpoint runs an Expectation Suite against a Batch (or Batch Request).

    Runs checkpoint after creation?
    """

    def __init__(
        self,
        ckan_name: str,
        checkpoint_name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.checkpoint_name = checkpoint_name

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        if self.checkpoint_name is None:
            self.checkpoint_name = ckan_context.default_checkpoint_name
        hook = GreatExpectationsHook(ckan_context.airflow_connection_id)
        # ge_context = hook.get_context()
        # suite = ge_context.get_expectation_suite(self.suite_name)
        checkpoint_config = dict(
            suite_name=ckan_context.suite_name,
            checkpoint_name=self.checkpoint_name,
        )
        checkpoint = hook.add_checkpoint(**checkpoint_config)
        # ge_context.save_expectation_suite(expectation_suite=suite)
        ckan_context.add_checkpoint_config(checkpoint_config)


class GeExecuteCheckpointOperator(CkanBaseOperator):

    def __init__(self, ckan_name: str, **kwargs):
        super().__init__(ckan_name=ckan_name, **kwargs)

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        hook = GreatExpectationsHook(ckan_context.airflow_connection_id)
        ge_context = hook.get_context()

        checkpoint_success = []  # Needed to store status of all checkpoints
        for checkpoint_name, configs in ckan_context.checkpoint_configs.items():

            # TODO: datasource must be generated again, because not persistet to GE store (workaround).
            datasource_config = ckan_context.checkpoint_configs[checkpoint_name]['batch_request']['datasource_config']
            _ = ge_context.add_datasource(**datasource_config, save_changes=True)

            # TODO: batch_request must be generated again, because not persistet to GE store (workaround).
            batch_request = BatchRequest(**configs['batch_request']['batch_request_config'])
            checkpoint_config = configs['checkpoint_config']

            # TODO: Does not use hook.py -> run_checkpont. Is this right?
            run_name = f'DAG {context.get("dag").safe_dag_id}'
            checkpoint_result = ge_context.run_checkpoint(
                checkpoint_name=checkpoint_config['checkpoint_name'],
                expectation_suite_name=checkpoint_config['suite_name'],
                batch_request=batch_request,
                run_name=run_name,
            )
            checkpoint_success.append(checkpoint_result.success)

        if all(checkpoint_success):
            ckan_context.checkpoint_success = True
        else:
            ckan_context.checkpoint_success = False

        ge_context.build_data_docs()
