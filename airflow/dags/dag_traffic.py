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
from ckanqa.operator.ckan import (CkanDeleteOperator, CkanExtractOperator,
                                  CkanParquetOperator, CkanDeleteContextOperator)
from ckanqa.operator.greatexpectations import (CkanContextSetter,
                                               GeBatchRequestOnS3Operator,
                                               GeBuildCheckpointOperator,
                                               GeBuildExpectationOperator,
                                               GeCheckSuiteOperator,
                                               GeExecuteCheckpointOperator,
                                               GeRemoveExpectationsOperator)

CURRENT_YEAR = str(dt.datetime.now().year)
CKAN_NAME = 'verkehrszahldaten-an-der-rosengartenstrasse-nach-fahrzeugtypen-seit-2020'

with DAG(
    dag_id='ckan_traffic',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2023, 1, 20),
    catchup=False,
    tags=['ckanqa', 'swiss', 'zurich'],
) as dag:

    t0 = CkanContextSetter(
        task_id='set_context',
        ckan_name=CKAN_NAME,
    )

    t1 = CkanExtractOperator(
        task_id='extract_ckan',
        ckan_name=CKAN_NAME,
        file_regex=r'.*' + CURRENT_YEAR + r'\.csv$'
    )

    t2 = CkanParquetOperator(
        task_id='convert_parquet',
        ckan_name=CKAN_NAME,
    )

    t3 = GeCheckSuiteOperator(
        task_id='check_suite',
        ckan_name=CKAN_NAME,
    )

    # Clear existing expectations, get recreated in the following tasks
    t3_2 = GeRemoveExpectationsOperator(
        task_id='remove_exp',
        ckan_name=CKAN_NAME,
    )

    t4 = GeBatchRequestOnS3Operator(
        task_id='datasource',
        ckan_name=CKAN_NAME,
        regex_filter=r'(.*)/(.*)/(.*)_(h\d)_(\d{4})\.parquet$',
        regex_filter_groups=['ckan_name', 'dag_run', 'data_asset_name', 'semester', 'year'],
        batch_request_data_asset_name='ugz_ogd_traffic_rosengartenbruecke',
    )

    # Can be executed in parallel, because do not get stored in CkanContext
    expectations = [
        GeBuildExpectationOperator(
            task_id='exp1',
            ckan_name=CKAN_NAME,
            expectation_type='expect_table_columns_to_match_ordered_list',
            ge_kwargs={
                'column_list': [
                    'Datum', 'Standort', 'Richtung', 'Spur', 'Klasse.ID',
                    'Klasse.Text', 'Intervall', 'Anzahl', 'Status'
                ],
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp2',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_values_to_be_dateutil_parseable',
            ge_kwargs={
                'column': 'Datum',
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp3',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_quantile_values_to_be_between',
            ge_kwargs={
                'column': 'Anzahl',
                'quantile_ranges': {
                    'quantiles': [0.5, 0.75],
                    'value_ranges': [[0, 10], [0, 50]]
                }
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp4',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_distinct_values_to_be_in_set',
            ge_kwargs={
                'column': 'Richtung',
                'value_set': ('Hardbrücke', 'Bucheggplatz')
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp5',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_values_to_be_between',
            ge_kwargs={
                'column': 'Spur',
                'min_value': 0,
                'max_value': 3,
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp6',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_values_to_be_between',
            ge_kwargs={
                'column': 'Anzahl',
                'min_value': 0,
                'max_value': 2_000,
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp7',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_values_to_be_dateutil_parseable',
            ge_kwargs={
                'column': 'Datum',
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp8',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_distinct_values_to_be_in_set',
            ge_kwargs={
                'column': 'Standort',
                'value_set': ('Zch_Rosengartenbrücke', )
            },
        ),
    ]

    t6 = GeBuildCheckpointOperator(
        task_id='add_checkpoint',
        ckan_name=CKAN_NAME,
    )

    t7 = GeExecuteCheckpointOperator(
        task_id='exec_checkpoint',
        ckan_name=CKAN_NAME,
    )

    t8 = CkanDeleteOperator(
        task_id='delete_files',
        ckan_name=CKAN_NAME,
    )

    t9 = CkanDeleteContextOperator(
        task_id='delete_context',
        ckan_name=CKAN_NAME,
    )

t0 >> t1 >> t2 >> t3 >> t3_2 >> t4 >> expectations >> t6 >> t7 >> t8 >> t9
