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

with DAG(
    dag_id='ckan_meteodaten',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
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
    CKAN_NAME = 'taglich-aktualisierte-meteodaten-seit-1992'
    CURRENT_YEAR = str(dt.datetime.now().year)
    REGEX_FILTER = r'(.*)\/(.*)\/(.*)_(.*)_(\d{4})__(.*)\.parquet$'
    REGEX_FILTER_GROUPS = ['ckan_name', 'dag_run', 'data_asset_name', 'interval', 'year', 'data_parameter']
    DATA_ASSET_NAME = 'ugz_ogd_meteo'

    validation_configs = [
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='RainDur',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'RainDur'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='T',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'T'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='T_max_h1',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'T_max_h1'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='p',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'p'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='StrGlo',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'StrGlo'}}
        ),
    ]

    expectation_mappings = [
        ############################ TABLE
        (
            ExpectationConfiguration(
                expectation_type='expect_table_columns_to_match_ordered_list',
                kwargs={
                    'column_list': [
                        'Datum', 'Standort', 'Parameter', 'Intervall', 'Einheit',
                        'Wert', 'Status'
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
                expectation_type='expect_column_values_to_match_regex',
                kwargs={
                    'column': 'Datum',
                    'regex': r'^\d{4}(?:-\d{2}){2}T\d{2}:\d{2}\+\d{4}$'
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Standort
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Standort',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Standort',
                    'value_set': ['Zch_Rosengartenstrasse', 'Zch_Schimmelstrasse', 'Zch_Stampfenbachstrasse'],
                }
            ), ()
        ),
        ############################ COL Parameter
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Parameter',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Parameter',
                    'value_set': ['RainDur', 'T', 'T_max_h1', 'p', 'StrGlo'],
                }
            ), ()
        ),
        ############################ COL Intervall
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Intervall',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Intervall',
                    'value_set': ['d1'],
                }
            ), ()
        ),
        ############################ COL Einheit
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Einheit',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Einheit',
                    'value_set': ['min'],
                }
            ), ('RainDur', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Einheit',
                    'value_set': ['°C'],
                }
            ), ('T', 'T_max_h1')
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Einheit',
                    'value_set': ['hPa'],
                }
            ), ('p', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_in_set',
                kwargs={
                    'column': 'Einheit',
                    'value_set': ['W/m2'],
                }
            ), ('StrGlo', )
        ),
        ############################ COL Wert
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 1440,
                }
            ), ('RainDur', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': -10,
                    'max_value': 40,
                }
            ), ('T', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': -5,
                    'max_value': 40,
                }
            ), ('T_max_h1', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 930,
                    'max_value': 1050,
                }
            ), ('p', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 400,
                }
            ), ('StrGlo', )
        ),
        ############################ COL Status
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Status',
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
        # split_by_column_group=['Parameter', 'Standort'],
        split_by_column_group=['Parameter'],
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
