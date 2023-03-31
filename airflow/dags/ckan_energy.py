import datetime as dt

from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from ckanqa.config import ValidationConfig
from ckanqa.factory import ValidationTaskFactory
from ckanqa.operator.ckan import (CkanDeleteContextOperator,
                                  CkanDeleteOperator, CkanExtractOperator,
                                  CkanParquetOperator)
from ckanqa.operator.context import CkanContextSetter

with DAG(
    dag_id='ckan_energy',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
    tags=[
        'ckanqa',  # project
        'stadt-winterthur'   # publisher org
    ],
) as dag:

    ##################################################################################################################
    # Subdags Factory
    ##################################################################################################################

    # Global variables
    CKAN_NAME = 'bruttolastgang-elektrische-energie-der-stadt-winterthur'
    REGEX_FILTER = r'(.*)\/(.*)\/(\w+)_(\d+)_(00003562)\.parquet$'
    REGEX_FILTER_GROUPS = ['ckan_name', 'dag_run', 'publisher', 'data_asset_name', 'data_asset_id']
    DATA_ASSET_NAME = '00001863'

    validation_configs = [
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='all_years',
            # data_connector_query={'batch_filter_parameters': {'location': 'tiefenbrunnen'}}
            data_connector_query={'batch_filter_parameters': {}}
        ),
    ]

    expectation_mappings = [
        # ########################### TABLE
        (
            ExpectationConfiguration(
                expectation_type='expect_table_columns_to_match_ordered_list',
                kwargs={
                    'column_list': [
                        'zeitpunkt', 'bruttolastgang_kwh',
                    ],
                }
            ), ()  # Apply to all validations
        ),
        # ########################### COL zeitpunkt
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'zeitpunkt',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_unique',
                kwargs={
                    'column': 'zeitpunkt',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_match_regex',
                kwargs={
                    'column': 'zeitpunkt',
                    'regex': r'^\d{4}(?:-\d{2}){2}T\d{2}(?::\d{2}){2}\+\d{2}:\d{2}$'
                }
            ), ()  # Apply to all validations
        ),
        # ########################### COL bruttolastgang_kwh
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'bruttolastgang_kwh',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'bruttolastgang_kwh',
                    'min_value': 5_000,
                    'max_value': 30_000,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'bruttolastgang_kwh',
                    'from_value': -2_000,
                    'to_value': 2_000,
                }
            ), ()
        ),
    ]

    factory = ValidationTaskFactory(
        ckan_name=CKAN_NAME,
        validation_configs=validation_configs,
        expectation_mappings=expectation_mappings,
    )
    factory.generate_tasks()

    # ################################################################################################################
    # DAG
    # ################################################################################################################
    t0 = CkanContextSetter(
        task_id='set_context',
        ckan_name=CKAN_NAME,
    )

    t1 = CkanExtractOperator(
        task_id='extract_ckan',
        ckan_name=CKAN_NAME,
        file_regex=r'.*\.csv$'
    )

    t2 = CkanParquetOperator(
        task_id='convert_parquet',
        ckan_name=CKAN_NAME,
    )

    t3 = DummyOperator(
        task_id='postprocessing'
    )

    # Add result = skipped, when no files were deleted
    t4 = CkanDeleteOperator(
        task_id='delete_files',
        ckan_name=CKAN_NAME,
    )

    t5 = CkanDeleteContextOperator(
        task_id='delete_context',
        ckan_name=CKAN_NAME,
    )

    _ = factory.connect_tasks(t2, t3)
    t0.set_downstream(t1)
    t1.set_downstream(t2)
    t3.set_downstream(t4)
    t4.set_downstream(t5)
