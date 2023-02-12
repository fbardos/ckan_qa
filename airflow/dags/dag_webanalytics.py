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
from ckanqa.operator.ckan import (CkanDeleteContextOperator,
                                  CkanDeleteOperator, CkanExtractOperator,
                                  CkanParquetOperator)
from ckanqa.operator.context import CkanContextSetter
from ckanqa.operator.greatexpectations import (GeBatchRequestOnS3Operator,
                                               GeBuildCheckpointOperator,
                                               GeBuildExpectationOperator,
                                               GeCheckSuiteOperator,
                                               GeExecuteCheckpointOperator,
                                               GeRemoveExpectationsOperator)

CKAN_NAME = 'web-analytics-des-webauftritts-des-kantons-zurich-ab-juli-2020'

with DAG(
    dag_id='ckan_webanalytics',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
    tags=['ckanqa', 'swiss', 'staatskanzlei-kanton-zuerich'],
) as dag:

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
        task_id='convert_to_parquet',
        ckan_name=CKAN_NAME,
    )

    t3 = GeCheckSuiteOperator(
        task_id='check_for_suite',
        ckan_name=CKAN_NAME,
    )

    t3_2 = GeRemoveExpectationsOperator(
        task_id='remove_exp',
        ckan_name=CKAN_NAME,
    )

    t4 = GeBatchRequestOnS3Operator(
        task_id='datasource',
        ckan_name=CKAN_NAME,
        regex_filter=r'(.*)/(.*)/(.*)\.parquet$',
        regex_filter_groups=['ckan_name', 'dag_run', 'data_asset_name'],
        data_asset_name='data_stat',
    )

    expectations = [
        GeBuildExpectationOperator(
            task_id='exp1',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_values_to_match_regex_list',
            ge_meta={
                "notes": {
                    "format": "markdown",
                    "content": "Column `url` should be a valid URL."
                }
            },
            ge_kwargs={
                'column': 'url',
                'regex_list': [
                    r'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)',
                ]
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp2',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_values_to_be_dateutil_parseable',
            ge_meta={
                "notes": {
                    "format": "markdown",
                    "content": "Column `date` should be dateutil parseable."
                }
            },
            ge_kwargs={
                'column': 'date',
            }
        ),
        GeBuildExpectationOperator(
            task_id='exp3',
            ckan_name=CKAN_NAME,
            expectation_type='expect_column_quantile_values_to_be_between',
            ge_meta={
                "notes": {
                    "format": "markdown",
                    "content": "Example *markdown* `note`."
                }
            },
            ge_kwargs={
                'column': 'nb_visits',
                'quantile_ranges': {
                    'quantiles': [0.25, 0.75],
                    'value_ranges': [[1, 3], [5, 20]]
                }
            }
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
