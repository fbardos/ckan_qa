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
    dag_id='ckan_luftqualitaet',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
    tags=[
        'ckanqa',  # project
        'stadt-zurich'  # publisher org
    ],
) as dag:

    ##################################################################################################################
    # Subdags Factory
    ##################################################################################################################

    # Global variables
    CKAN_NAME = 'taglich-aktualisierte-luftqualitatsmessungen-seit-1983'
    CURRENT_YEAR = str(dt.datetime.now().year)
    REGEX_FILTER = r'(.*)\/(.*)\/(.*)_(.*)_(\d{4})__(.*)\.parquet$'
    # REGEX_FILTER = r'(.*)\/(.*)\/(.*)_(.*)_(\d{4})__(.*)__(.*)\.parquet$'
    REGEX_FILTER_GROUPS = ['ckan_name', 'dag_run', 'data_asset_name', 'interval', 'year', 'data_parameter']
    DATA_ASSET_NAME = 'ugz_ogd_air'

    validation_configs = [
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='NOx',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'NOx'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='NO',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'NO'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='NO2',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'NO2'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='O3',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'O3'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='O3_max_h1',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'O3_max_h1'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='PM10',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'PM10'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='PM2.5',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'PM2.5'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='PN',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'PN'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='SO2',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'SO2'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='CO',
            data_connector_query={'batch_filter_parameters': {'data_parameter': 'CO'}}
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
                    'column': 'Datum'
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
        ############################ COL Standort
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Standort',
                }
            ), ()  # Apply to all validations
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_match_regex',
                kwargs={
                    'column': 'Standort',
                    'regex': r'^Zch_.*',
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Parameter
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Parameter',
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Intervall
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Intervall',
                }
            ), ()  # Apply to all validations
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_distinct_values_to_equal_set',
                kwargs={
                    'column': 'Intervall',
                    'value_set': ['d1']
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Einheit
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'Einheit',
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL Wert
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 100,
                }
            ), ('NOx', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 90,
                }
            ), ('NO', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 100,
                }
            ), ('NO2', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 100,
                }
            ), ('O3', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 120,
                }
            ), ('O3_max_h1', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 80,
                }
            ), ('PM10', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 80,
                }
            ), ('PM2.5', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 500,
                    'max_value': 50_000,
                }
            ), ('PN', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 10,
                }
            ), ('SO2', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 1,
                }
            ), ('CO', )
        ),
        ############################ COL Status
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
