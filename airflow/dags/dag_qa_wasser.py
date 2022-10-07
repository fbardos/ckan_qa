import datetime as dt
import os
import sys

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Use relative path for custom modules (easier to handle with airflow deployment atm)
sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../../../ckanqa'))))
from ckanqa.ckan import CkanPropagateResultMatrix, CkanRedisOperatorFactory
from ckanqa.expectation import *

CKAN_META = 'https://ckan.opendata.swiss/api/3/action/package_show?id=messwerte-der-wetterstationen-der-wasserschutzpolizei-zurich2'

with DAG(
    dag_id='ckan_qa_wasser',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
    tags=['ckan', 'swiss']
) as dag:

    # FIX: Look at key in redis --> is wrong, does not contain the filename as key
    factory = CkanRedisOperatorFactory(CKAN_META)
    load = factory.create_store_operator(
        task_id='load',
        csv_pattern='^.*_{Y}\.csv'
    )

    checks = [
        ExpectTableColumnsToMatchOrderedListOperator(
            task_id='check1',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column_list': [
                    'timestamp_utc','timestamp_cet','air_temperature','water_temperature','wind_gust_max_10min',
                    'wind_speed_avg_10min','wind_force_avg_10min','wind_direction','windchill',
                    'barometric_pressure_qfe','precipitation','dew_point','global_radiation','humidity','water_level',
                ]
            }
        ),
        ExpectColumnValuesToBeDateutilParseable(
            task_id='check2',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'timestamp_utc',
            }
        ),
        ExpectColumnValuesToBeDateutilParseable(
            task_id='check3',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'timestamp_cet',
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check4',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'air_temperature',
                'min_value': -20,
                'max_value': 40,
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check5',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'water_temperature',
                'min_value': 0,
                'max_value': 30,
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check6',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'wind_gust_max_10min',
                'min_value': 0,
                'max_value': 194,
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check7',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'wind_speed_avg_10min',
                'min_value': 0,
                'max_value': 194,
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check8',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'wind_direction',
                'min_value': 0,
                'max_value': 359,
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check9',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'windchill',
                'min_value': -80,
                'max_value': 10,
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check10',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'barometric_pressure_qfe',
                'min_value': 891,
                'max_value': 1070,
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check11',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'dew_point',
                'min_value': -10,
                'max_value': 10,
            }
        ),
        ExpectColumnMedianToBeBetween(
            task_id='check12',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'humidity',
                'min_value': 65,
                'max_value': 90,
            }
        ),
    ]

    postprocessing = EmptyOperator(task_id='postprocessing')

    clean = factory.create_delete_operator(
        task_id='clean',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    notify_matrix_all = CkanPropagateResultMatrix(task_id='notify_matrix_all', only_failed=False, short=True)
    notify_matrix_failed = CkanPropagateResultMatrix(task_id='notify_matrix_failed', only_failed=True, short=True)

    load >> [*checks] >> postprocessing >> [clean, notify_matrix_all, notify_matrix_failed]
