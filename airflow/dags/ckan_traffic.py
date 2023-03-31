"""
DAG of v.2 should consist of:

    - Store CKAN metadata in Redis
    - Load data from CKAN into S3 (CSV)
    - Convert to parquet file
    - Check whether checkpoint / expectations are already stored in GE (if so, then skip)
    - Execute validation
    - Notify if error
    - Build / publish docs
    - (delete parkquet file) --> maybe do not delete when there was an error

Without spark, but with minio. Can be changed later.

"""
import datetime as dt

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from ckanqa.operator.ckan import (CkanDeleteContextOperator,
                                  CkanDeleteOperator, CkanExtractOperator,
                                  CkanParquetOperator)
from ckanqa.operator.context import CkanContextSetter
from ckanqa.factory import ValidationTaskFactory
from ckanqa.config import ValidationConfig

from great_expectations.core.expectation_configuration import ExpectationConfiguration

import datetime as dt

from airflow import DAG
from ckanqa.operator.ckan import (CkanDeleteContextOperator,
                                  CkanDeleteOperator, CkanExtractOperator,
                                  CkanParquetOperator)
from ckanqa.operator.context import CkanContextSetter

with DAG(
    dag_id='ckan_traffic',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2023, 1, 20),
    catchup=False,
    tags=[
        'ckanqa',  # project
        'stadt-zurich'   # publisher org
    ],
) as dag:

    ##################################################################################################################
    # Subdags Factory
    ##################################################################################################################

    # Global variables
    CKAN_NAME = 'verkehrszahldaten-an-der-rosengartenstrasse-nach-fahrzeugtypen-seit-2020'
    CURRENT_YEAR = str(dt.datetime.now().year)
    REGEX_FILTER = r'(.*)\/(.*)\/(.*)_(\w*)_(\w\d)_(\d{4})__(\w*)\.parquet$'
    REGEX_FILTER_GROUPS = ['ckan_name', 'dag_run', 'data_asset_name', 'location', 'period', 'year', 'richtung']
    DATA_ASSET_NAME = 'ugz_ogd_traffic'

    validation_configs = [
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='hardbruecke',
            data_connector_query={'batch_filter_parameters': {'richtung': 'Hardbrücke'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='bucheggplatz',
            data_connector_query={'batch_filter_parameters': {'richtung': 'Bucheggplatz'}}
        ),
    ]

    expectation_mappings = [
        ############################ TABLE
        (
            ExpectationConfiguration(
                expectation_type='expect_table_columns_to_match_ordered_list',
                kwargs={
                    'column_list': [
                        'Datum', 'Standort', 'Richtung', 'Spur', 'Klasse.ID',
                        'Klasse.Text', 'Intervall', 'Anzahl', 'Status'
                    ],
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Datum
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Datum',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_dateutil_parseable',
                kwargs={
                    'column': 'Datum',
                }
            ), ()  # Apply to all validations
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_match_regex',
                kwargs={
                    'column': 'Datum',
                    'regex': r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}\+\d{4}$',
                    'result_format': 'COMPLETE'
                }
            ), ()  # Apply to all validations
        ),
        # (
            # ExpectationConfiguration(
                # expectation_type='expect_column_values_to_match_strftime_format',
                # kwargs={
                    # 'column': 'Datum',
                    # 'strftime_format': '%Y-%m-%dT%H:%M%z',
                    # 'catch_exceptions': True,
                    # # 'strftime_format': '%Y-%m-%dT%H:%M',
                # }
            # ), ()  # Apply to all validations
        # ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_increasing',
                kwargs={
                    'column': 'Datum',
                    'parse_strings_as_datetimes': True,
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Standort
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Standort'
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Status',
                    'value_set': ['Zch_Rosengartenbrücke']
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Richtung
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Richtung'
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Status',
                    'value_set': ['Bucheggplatz']
                }
            ), ('bucheggplatz', )  # Apply to all validations
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Richtung',
                    'value_set': ['Hardbrücke']
                }
            ), ('hardbruecke', )  # Apply to all validations
        ),
        ############################ COL Spur
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Spur',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Spur',
                    'value_set': [0, 1, 2, 3],
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Klasse.ID
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Klasse.ID',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_of_type',
                kwargs={
                    'column': 'Klasse.ID',
                    'type_': 'int64',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Klasse.ID',
                    'min_value': 0,
                    'max_value': 11,
                }
            ), ()
        ),
        (
            # Every distinct Klasse.ID should be evently distributed.
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'Klasse.ID',
                    'min_value': 5.4,
                    'max_value': 5.6,
                }
            ), ()
        ),
        ############################ COL Klasse.Text
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Klasse.Text'
                }
            ), ()
        ),
        ############################ COL Intervall
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Intervall'
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Intervall',
                    'value_set': ['h1'],
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Anzahl
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Anzahl'
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Anzahl',
                    'min_value': 0,
                    'max_value': 1_500,
                }
            ), ()
        ),
        ############################ COL Status
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Status'
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Status',
                    'value_set': ['provisorisch', 'bereinigt']
                }
            ), ()  # Apply to all validations
        ),
    ]

    factory = ValidationTaskFactory(
        ckan_name=CKAN_NAME,
        validation_configs=validation_configs,
        expectation_mappings=expectation_mappings,
    )
    factory.generate_tasks()

    ##################################################################################################################
    # DAG
    ##################################################################################################################
    t0 = CkanContextSetter(
        task_id='set_context',
        ckan_name=CKAN_NAME,
    )

    t1 = CkanExtractOperator(
        task_id='extract_ckan',
        ckan_name=CKAN_NAME,
        file_regex=fr'.*{CURRENT_YEAR}\.csv$'
    )

    t2 = CkanParquetOperator(
        task_id='convert_parquet',
        ckan_name=CKAN_NAME,

        # Due to https://github.com/great-expectations/great_expectations/issues/6131:
        # At the moment, only one batch gets selected when data_asset_query is set (instead of like 4 for NOx).
        # Therefore, as a temporary workaround, do not split by multiple Standort and wait until issue is resolved.
        # It doesnt matter, if data_connector_query is set or not. Either way, only one Batch (file) gets selected.
        split_by_column_group=['Richtung'],
        expected_column_groups=['Bucheggplatz', 'Hardbrücke'],
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
