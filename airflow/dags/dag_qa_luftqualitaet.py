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

CKAN_META = 'https://ckan.opendata.swiss/api/3/action/package_show?id=taglich-aktualisierte-luftqualitatsmessungen-seit-1983'

with DAG(
    dag_id='ckan_qa_luftqualitaet',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
    tags=['ckan', 'swiss'],
) as dag:

    factory = CkanRedisOperatorFactory(CKAN_META)
    load = factory.create_store_operator(
        task_id='load',
    )

    checks = [
        ExpectTableColumnsToMatchOrderedListOperator(
            task_id='check1',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column_list': ['Datum', 'Standort', 'Parameter', 'Intervall', 'Einheit', 'Wert', 'Status']
            }
        ),
        ExpectColumnDistinctValuesToBeInSet(
            task_id='check2',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Status',
                'value_set': ['provisorisch', 'bereinigt'],
            }
        ),
        ExpectColumnValuesToBeDateutilParseable(
            task_id='check3',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Datum',
            },
        ),
        ExpectColumnDistinctValuesToBeInSet(
            task_id='check4',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Parameter',
                'value_set': ['CO','NO2','NO','NOx','O3','O3_max_h1','O3_nb_h1>120','PM10','PM2.5','PN','SO2'],
            },
        ),
        ExpectColumnDistinctValuesToBeInSet(
            task_id='check5',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Intervall',
                'value_set': ['d1'],
            },
        ),

        ExpectColumnDistinctValuesToBeInSet(
            task_id='check6',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Einheit',
                'value_set': ['µg/m3', 'ppb', '1', '1/cm3', 'mg/m3'],
            },
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check7',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 1,
                'max_value': 1000,
            },
            df_query_str='Parameter == "NOx"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check8',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0,
                'max_value': 200,
            },
            df_query_str='Parameter == "O3"',
        ),
        ExpectColumnMedianToBeBetween(
            task_id='check9',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0.15,
                'max_value': 2.9,
            },
            df_query_str='Parameter == "CO"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check10',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0,
                'max_value': 470,
            },
            df_query_str='Parameter == "SO2"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check11',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 1.1,
                'max_value': 280,
            },
            df_query_str='Parameter == "NO2"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check12',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0.02,
                'max_value': 750,
            },
            df_query_str='Parameter == "NO"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check13',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0,
                'max_value': 300,
            },
            df_query_str='Parameter == "O3_max_h1"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check14',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0,
                'max_value': 22,
            },
            df_query_str='Parameter == "O3_nb_h1>120"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check15',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0.9,
                'max_value': 170,
            },
            df_query_str='Parameter == "PM10"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check16',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 1.9,
                'max_value': 70,
            },
            df_query_str='Parameter == "PM2.5"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check17',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0,
                'max_value': 80_000,
            },
            df_query_str='Parameter == "PN"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check18',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0,
            },
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
