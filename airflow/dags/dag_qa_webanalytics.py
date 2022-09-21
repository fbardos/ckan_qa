import datetime as dt
import os
import sys

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Use relative path for custom modules (easier to handle with airflow deployment atm)
sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../../../ckanqa'))))
from ckanqa.ckan import (CkanCsvDeleteOperator, CkanCsvStoreOperator,
                         CkanPropagateResultMatrix)
from ckanqa.expectation import *

CKAN_META = 'https://ckan.opendata.swiss/api/3/action/package_show?id=web-analytics-des-webauftritts-des-kantons-zurich-ab-juli-2020'


with DAG(
    dag_id='ckan_qa_webanalytics',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
) as dag:

    load = CkanCsvStoreOperator(
        task_id='load',
        ckan_metadata_url=CKAN_META
    )

    checks = [
        ExpectColumnValuesToMatchRegexList(
            task_id='check1',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'url',
                'regex_list': [
                    r'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)'
                ],
            }
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check2',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'nb_visits',
                'min_value': 1,
            }
        ),
    ]

    postprocessing = EmptyOperator(task_id='postprocessing')

    clean = CkanCsvDeleteOperator(
        task_id='clean',
        trigger_rule=TriggerRule.ALL_DONE,
        ckan_metadata_url=CKAN_META,
    )

    notify_matrix_all = CkanPropagateResultMatrix(task_id='notify_matrix_all', only_failed=False, short=True)
    notify_matrix_failed = CkanPropagateResultMatrix(task_id='notify_matrix_failed', only_failed=True, short=True)

    load >> [*checks] >> postprocessing >> [clean, notify_matrix_all, notify_matrix_failed]
