import os
import sys
import datetime as dt

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

# Use relative path for custom modules (easier to handle with airflow deployment atm)
sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../../../ckanqa'))))
from ckanqa.ckan import CkanCsvDeleteOperator, CkanCsvStoreOperator
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
            ge_col='url',
            ge_regex_list=[
                r'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)'],
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check2',
            ge_col='nb_visits',
            ge_min=1,
            ckan_metadata_url=CKAN_META,
        ),
    ]

    clean = CkanCsvDeleteOperator(
        task_id='clean',
        trigger_rule=TriggerRule.ALL_DONE,
        ckan_metadata_url=CKAN_META,
    )

    load >> [*checks] >> clean
