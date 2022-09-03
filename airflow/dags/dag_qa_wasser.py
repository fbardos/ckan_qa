import os
import sys
import datetime as dt

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

# Use relative path for custom modules (easier to handle with airflow deployment atm)
sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../../../ckanqa'))))
from ckanqa.ckan import CkanCsvDeleteOperator, CkanCsvStoreOperator
from ckanqa.expectation import *


CKAN_META = 'https://ckan.opendata.swiss/api/3/action/package_show?id=messwerte-der-wetterstationen-der-wasserschutzpolizei-zurich2'
_year = dt.datetime.now(tz=dt.timezone.utc).year
_CSV_URLS = [
    f'https://data.stadt-zuerich.ch/dataset/sid_wapo_wetterstationen/download/messwerte_tiefenbrunnen_{_year}.csv',
    f'https://data.stadt-zuerich.ch/dataset/sid_wapo_wetterstationen/download/messwerte_mythenquai_{_year}.csv',
]

with DAG(
    dag_id='ckan_qa_wasser',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
) as dag:

    load = CkanCsvStoreOperator(
        task_id='load',
        ckan_metadata_url=CKAN_META,
        extract_csv_urls=_CSV_URLS,
    )

    checks = [
        ExpectTableColumnsToMatchOrderedListOperator(
            task_id='check1',
            ge_col_list=[
                'timestamp_utc','timestamp_cet','air_temperature','water_temperature','wind_gust_max_10min',
                'wind_speed_avg_10min','wind_force_avg_10min','wind_direction','windchill',
                'barometric_pressure_qfe','precipitation','dew_point','global_radiation','humidity','water_level',
            ],
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeDateutilParseable(
            task_id='check2',
            ge_col='timestamp_utc',
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeDateutilParseable(
            task_id='check3',
            ge_col='timestamp_cet',
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check4',
            ge_col='air_temperature',
            ge_min=-20,
            ge_max=40,
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check5',
            ge_col='water_temperature',
            ge_min=0,
            ge_max=30,
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check6',
            ge_col='wind_gust_max_10min',
            ge_min=0,
            ge_max=194,  # According to Wikipedia max classified speed: https://de.wikipedia.org/wiki/Windgeschwindigkeit
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check7',
            ge_col='wind_speed_avg_10min',
            ge_min=0,
            ge_max=194,  # According to Wikipedia max classified speed: https://de.wikipedia.org/wiki/Windgeschwindigkeit
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check8',
            ge_col='wind_direction',
            ge_min=0,
            ge_max=359,
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check9',
            ge_col='windchill',
            ge_min=-80,
            ge_max=10,  # According to https://de.wikipedia.org/wiki/Windchill
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check10',
            ge_col='barometric_pressure_qfe',
            ge_min=891,
            ge_max=1070,  # According to https://de.wikipedia.org/wiki/Luftdruck
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check11',
            ge_col='dew_point',
            ge_min=-10,
            ge_max=10,
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnMedianToBeBetween(
            task_id='check12',
            ge_col='humidity',
            ge_min=65,
            ge_max=90,
            ckan_metadata_url=CKAN_META,
        ),
    ]

    clean = CkanCsvDeleteOperator(
        task_id='clean',
        trigger_rule=TriggerRule.ALL_DONE,
        ckan_metadata_url=CKAN_META,
    )

    load >> [*checks] >> clean
