"""
Contains Operators for Airflow.
"""
import logging
import os
from collections.abc import Sequence
from typing import Optional, Sequence

from dotenv import load_dotenv
from great_expectations.core.batch import BatchMarkers, BatchRequest
from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration
from ruamel import yaml

from ckanqa.config import ValidationConfig
from ckanqa.context import CkanContext
from ckanqa.hook import GreatExpectationsHook
from ckanqa.operator.ckan import CkanBaseOperator


load_dotenv()
S3_BUCKET_NAME_META = os.environ['CKANQA__CONFIG__S3_BUCKET_NAME_META']


class CkanBaseOperatorMeta(CkanBaseOperator):

    def __init__(self, ckan_name: str, **kwargs):
        super().__init__(ckan_name=ckan_name, **kwargs)
        if v := kwargs.get('bucket_name', None):
            self.bucket_name = v
        else:
            self.bucket_name = S3_BUCKET_NAME_META

    def build_ge_hook(self, ckan_context: CkanContext) -> GreatExpectationsHook:
        return GreatExpectationsHook(ckan_context.airflow_connection_id, bucket_name=self.bucket_name)


class GeBuildExpectationOperator(CkanBaseOperatorMeta):
    """Register Ge datasource via pandas DataFrame.

    Suite name gets automatically generated.

    """
    def __init__(
        self,
        ckan_name: str,
        suite_name: str,
        expectation_config: ExpectationConfiguration | Sequence[ExpectationConfiguration],
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.suite_name = suite_name
        if isinstance(expectation_config, ExpectationConfiguration):
            self.expectation_config = [expectation_config]
        else:
            self.expectation_config = expectation_config

    def execute(self, context):
        """

        Does not save any config to ckan_context, otherwise, not parallelization
        would be possible.

        """
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        hook = self.build_ge_hook(ckan_context)
        _ = hook.get_suite(self.suite_name)

        # No suite saving requred. Gets automatically persisted.
        for expectation_config in self.expectation_config:
            _ = hook.add_expectation(self.suite_name, expectation_config)


class GeRemoveExpectationsOperator(CkanBaseOperatorMeta):
    """ Removes existing expectation, for later recreation.

    Args:
        - expectation_configuration: If not set, every expectation gets deleted.

    """

    def __init__(
        self,
        ckan_name: str,
        suite_name: str,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        remove_multiple_matches: bool = True,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.suite_name = suite_name
        self.remove_multiple_matches = remove_multiple_matches
        if expectation_configuration:
            self.expectation_configuration = expectation_configuration
        else:

            # When no expectation_configuration is given, all expectations
            # will be deleted.
            self.expectation_configuration = {}

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        hook = self.build_ge_hook(ckan_context)
        ge_context = hook.get_base_data_context()
        suite = hook.get_suite(self.suite_name)
        logging.debug(f'Currently saved expectations: {suite.expectations}')
        if len(suite.expectations) == 0:
            logging.warning('There were no saved expectations. Skip.')
        else:
            suite.remove_expectation(
                expectation_configuration=self.expectation_configuration,
                remove_multiple_matches=self.remove_multiple_matches,
            )
            ge_context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=self.suite_name)


class GeRunValidator(CkanBaseOperatorMeta):
    """Version 2

    One per data asset.
        --> One per combination of:
            - ValidationConfig.data_connector_query
            - ValidationConfig.data_asset_name

    Currently, only reading from csv/parquet is supported. Maybe, it makes sense to also allow
    pandas DataFrames to be passed.

    """

    def __init__(
        self,
        validation_config: ValidationConfig,
        s3_prefix: Optional[str] = None,
        **kwargs
    ):
        self.validation_config = validation_config
        super().__init__(ckan_name=self.validation_config.ckan_name, **kwargs)

        self.s3_prefix = s3_prefix

    def _generate_datasource_config(
        self,
        datasource_name: str,
        data_connector_name: str,
        data_bucket_name: str,
        s3_prefix: str,
        boto3_options: dict,
    ) -> dict:
        """Currently only supports S3"""
        bucket = data_bucket_name
        prefix = s3_prefix
        datasource_config = {
            'name': datasource_name,
            'class_name': 'Datasource',
            'execution_engine': {
                'class_name': 'PandasExecutionEngine',
                'boto3_options': boto3_options,
            },
            'data_connectors': {
                data_connector_name: {
                    'class_name': 'InferredAssetS3DataConnector',
                    'bucket':  bucket if not bucket.endswith('/') else bucket + '/',
                    'prefix': prefix if not prefix.endswith('/') else prefix + '/',
                    'boto3_options': boto3_options,
                    'default_regex': {
                        'pattern': self.validation_config.regex_filter,
                        # You can assign group names to regex groups here.
                        # With this regex, you can assign data_asset_name
                        # to a group of the regex search.
                        'group_names': self.validation_config.regex_filter_groups,
                    },
                },
            },
        }
        return datasource_config

    def execute(self, context):

        # According to GE Docs, when using an experimental expectation, an import is needed
        # when Expectation Suite is created AND when checkpoint is defined and run.
        from great_expectations_experimental.expectations.expect_column_values_to_change_between import \
            ExpectColumnValuesToChangeBetween

        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        hook = self.build_ge_hook(ckan_context)
        ge_context = hook.get_base_data_context()
        # suite = hook.get_suite(self.validation_config.suite_name)

        if self.s3_prefix is None:
            self.s3_prefix = ckan_context.default_s3_data_prefix

        # Datasource
        datasource_config = self._generate_datasource_config(
            datasource_name=ckan_context.default_datasource_name,
            data_connector_name=ckan_context.default_data_connector_name,
            data_bucket_name=ckan_context.data_bucket_name,
            s3_prefix=self.s3_prefix,
            boto3_options=ckan_context.boto3_options,
        )
        ge_context.test_yaml_config(yaml.dump(datasource_config))
        ge_context.add_datasource(
            **datasource_config,
            save_changes=True
        )

        # BatchRequest
        batch_markers = BatchMarkers(
            {
                'airflow_dagrun': context.get('dag_run').run_id,
                # Not sure if this is the intended way to do it. In Data Docs --> Validation info,
                # ge_load_time is listed twice.
                'ge_load_time': self.get_dag_runtime_iso_8601_basic(context),  # overwritten from default behaviour
            }
        )

        batch_request = BatchRequest(
            datasource_name=ckan_context.default_datasource_name,
            data_connector_name=ckan_context.default_data_connector_name,
            data_asset_name=self.validation_config.data_asset_name,
            data_connector_query=self.validation_config.data_connector_query,
            batch_spec_passthrough={'batch_markers': batch_markers}
        )

        # For GreatExpectations > 0.13.8, validating data without any checkpoint is
        # not supported anymore. Therefore, we have to create a checkpoint first.
        checkpoint_name = ckan_context.build_valiation_checkpoint_name(self.validation_config.validation_name)
        suite_name = self.validation_config.suite_name
        _ = hook.add_or_update_checkpoint(
            checkpoint_name=checkpoint_name,
            suite_name=suite_name
        )

        run_name = f'DAG {context.get("dag").safe_dag_id}, VALIDATION {self.validation_config.validation_name}'
        checkpoint_result = ge_context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            expectation_suite_name=suite_name,
            batch_request=batch_request,
            run_name=run_name,
        )

        # Build data docs (otherwise, expectation suites do not get correctly displayed in data docs)
        ge_context.build_data_docs()

        # Append element to redis with db locking
        ckan_context.append_checkpoint_success(checkpoint_result.success)
