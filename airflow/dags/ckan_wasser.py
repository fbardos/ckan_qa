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
    dag_id='ckan_wasser',
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
    CKAN_NAME = 'messwerte-der-wetterstationen-der-wasserschutzpolizei-zurich2'
    REGEX_FILTER = r'(.*)\/(.*)\/(\w*)_(\w*)_seit(\d{4})-heute\.parquet$'
    REGEX_FILTER_GROUPS = ['ckan_name', 'dag_run', 'data_asset_name', 'location', 'year']
    DATA_ASSET_NAME = 'messwerte'

    validation_configs = [
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='tiefenbrunnen',
            data_connector_query={'batch_filter_parameters': {'location': 'tiefenbrunnen'}}
        ),
        ValidationConfig(
            ckan_name=CKAN_NAME,
            data_asset_name=DATA_ASSET_NAME,
            regex_filter=REGEX_FILTER,
            regex_filter_groups=REGEX_FILTER_GROUPS,
            validation_name='mythenquai',
            data_connector_query={'batch_filter_parameters': {'location': 'mythenquai'}}
        ),
    ]

    expectation_mappings = [
        ############################ TABLE
        (
            ExpectationConfiguration(
                expectation_type='expect_table_columns_to_match_ordered_list',
                kwargs={
                    'column_list': [
                        'timestamp_utc', 'timestamp_cet', 'air_temperature', 'water_temperature',
                        'wind_gust_max_10min', 'wind_speed_avg_10min', 'wind_force_avg_10min',
                        'wind_direction', 'windchill', 'barometric_pressure_qfe', 'precipitation',
                        'dew_point', 'global_radiation', 'humidity', 'water_level',
                    ],
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL timestamp_utc
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'timestamp_utc',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_unique',
                kwargs={
                    'column': 'timestamp_utc',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_match_regex',
                kwargs={
                    'column': 'timestamp_utc',
                    'regex': r'^\d{4}(?:-\d{2}){2}T\d{2}(?::\d{2}){2}\+\d{2}:\d{2}$'
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL timestamp_cet
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'timestamp_cet',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_unique',
                kwargs={
                    'column': 'timestamp_cet',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_match_regex',
                kwargs={
                    'column': 'timestamp_utc',
                    'regex': r'^\d{4}(?:-\d{2}){2}T\d{2}(?::\d{2}){2}\+\d{2}:\d{2}$'
                }
            ), ()  # Apply to all validations
        ),
        ############################ COL air_temperature
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'air_temperature',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'air_temperature',
                    'min_value': -20,
                    'max_value': 40,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'air_temperature',
                    'min_value': 9,
                    'max_value': 15,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            # Unfortunately 'expect_column_values_to_change_between' is experimental at the moment.
            # Therefore, have to switch to another expectation type.
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'air_temperature',
                    'from_value': -13,
                    'to_value': 13,
                }
            ), ()
        ),
        ############################ COL water_temperature
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'water_temperature',
                    'mostly': 0.85,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_quantile_values_to_be_between',
                kwargs={
                    'column': 'water_temperature',
                    'quantile_ranges': {
                        'quantiles': [0.25, 0.75],
                        'value_ranges': [[5, 8], [17, 21]],
                    },
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'water_temperature',
                    'min_value': 11,
                    'max_value': 15,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        ############################ COL wind_gust_max_10min
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'wind_gust_max_10min',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'wind_gust_max_10min',
                    'min_value': -0.1,
                    'max_value': 40,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'wind_gust_max_10min',
                    'min_value': 2,
                    'max_value': 4,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'wind_gust_max_10min',
                    'from_value': -35,
                    'to_value': 35,
                }
            ), ()
        ),
        ############################ COL wind_speed_avg_10min
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'wind_speed_avg_10min',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'wind_speed_avg_10min',
                    'min_value': 0,
                    'max_value': 25,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'wind_speed_avg_10min',
                    'min_value': 1,
                    'max_value': 2,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'wind_speed_avg_10min',
                    'from_value': -20,
                    'to_value': 20,
                }
            ), ()
        ),
        ############################ COL wind_force_avg_10min
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'wind_force_avg_10min',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'wind_force_avg_10min',
                    'min_value': 0,
                    'max_value': 25,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'wind_force_avg_10min',
                    'min_value': 1,
                    'max_value': 2,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'wind_force_avg_10min',
                    'from_value': -20,
                    'to_value': 20,
                }
            ), ()
        ),
        ############################ COL wind_direction
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'wind_direction',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'wind_direction',
                    'min_value': 0,
                    'max_value': 360,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'wind_direction',
                    'min_value': 140,
                    'max_value': 200,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        ############################ COL windchill
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'windchill',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'windchill',
                    'min_value': -30,
                    'max_value': 100,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'windchill',
                    'min_value': 5,
                    'max_value': 15,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'windchill',
                    'from_value': -70,
                    'to_value': 70,
                }
            ), ()
        ),
        ############################ COL barometric_pressure_qfe
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'barometric_pressure_qfe',
                    'mostly': 0.9,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'barometric_pressure_qfe',
                    'min_value': 900,
                    'max_value': 1080,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'barometric_pressure_qfe',
                    'min_value': 960,
                    'max_value': 980,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'barometric_pressure_qfe',
                    'from_value': -70,
                    'to_value': 70,
                }
            ), ()
        ),
        ############################ COL precipitation
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_null',
                kwargs={
                    'column': 'precipitation',
                }
            ), ('tiefenbrunnen', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'precipitation',
                    'min_value': 0,
                    'max_value': 20,
                }
            ), ('mythenquai', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'precipitation',
                    'min_value': 0.01,
                    'max_value': 0.021,
                    'result_format': 'COMPLETE',
                }
            ), ('mythenquai', )
        ),
        ############################ COL dew_point
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'dew_point',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'dew_point',
                    'min_value': -20,
                    'max_value': 80,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'dew_point',
                    'min_value': 6,
                    'max_value': 7,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'dew_point',
                    'from_value': -60,
                    'to_value': 60,
                }
            ), ()
        ),
        ############################ COL global_radiation
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_null',
                kwargs={
                    'column': 'global_radiation',
                }
            ), ('tiefenbrunnen', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'global_radiation',
                    'min_value': 0,
                    'max_value': 5000,
                }
            ), ('mythenquai', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'global_radiation',
                    'min_value': 100,
                    'max_value': 150,
                    'result_format': 'COMPLETE',
                }
            ), ('mythenquai', )
        ),
        ############################ COL humidity
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_not_be_null',
                kwargs={
                    'column': 'humidity',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'humidity',
                    'min_value': 15,
                    'max_value': 105,
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'humidity',
                    'min_value': 73,
                    'max_value': 77,
                    'result_format': 'COMPLETE',
                }
            ), ()
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'humidity',
                    'from_value': -40,
                    'to_value': 40,
                }
            ), ()
        ),
        ############################ COL water_level
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_null',
                kwargs={
                    'column': 'water_level',
                }
            ), ('tiefenbrunnen', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_be_between',
                kwargs={
                    'column': 'water_level',
                    'min_value': 400,
                    'max_value': 410,
                }
            ), ('mythenquai', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_mean_to_be_between',
                kwargs={
                    'column': 'water_level',
                    'min_value': 403,
                    'max_value': 407,
                    'result_format': 'COMPLETE',
                }
            ), ('mythenquai', )
        ),
        (
            ExpectationConfiguration(
                expectation_type='expect_column_values_to_change_between',
                kwargs={
                    'column': 'water_level',
                    'from_value': -1,
                    'to_value': 1,
                }
            ), ('mythenquai', )
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
