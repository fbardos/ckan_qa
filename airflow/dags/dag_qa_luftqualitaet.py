import os
import sys
import datetime as dt

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

# Use relative path for custom modules (easier to handle with airflow deployment atm)
sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../../../ckanqa'))))
from ckanqa.ckan import CkanCsvDeleteOperator, CkanCsvStoreOperator
from ckanqa.expectation import *


CKAN_META = 'https://ckan.opendata.swiss/api/3/action/package_show?id=taglich-aktualisierte-luftqualitatsmessungen-seit-1983'

with DAG(
    dag_id='ckan_qa_luftqualitaet',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
) as dag:

    load = CkanCsvStoreOperator(
        task_id='load',
        ckan_metadata_url=CKAN_META
    )

    checks = [
        ExpectTableColumnsToMatchOrderedListOperator(
            task_id='check1',
            ge_col_list=['Datum', 'Standort', 'Parameter', 'Intervall', 'Einheit', 'Wert', 'Status'],
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnDistinctValuesToBeInSet(
            task_id='check2',
            ge_col='Status',
            ge_values=['provisorisch', 'bereinigt'],
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeDateutilParseable(
            task_id='check3',
            ge_col='Datum',
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnDistinctValuesToBeInSet(
            task_id='check4',
            ge_col='Parameter',
            ge_values=['CO','NO2','NO','NOx','O3','O3_max_h1','O3_nb_h1>120','PM10','PM2.5','PN','SO2'],
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnDistinctValuesToBeInSet(
            task_id='check5',
            ge_col='Intervall',
            ge_values=['d1'],
            ckan_metadata_url=CKAN_META,
        ),

        ExpectColumnDistinctValuesToBeInSet(
            task_id='check6',
            ge_col='Einheit',
            ge_values=['µg/m3', 'ppb', '1', '1/cm3', 'mg/m3'],
            ckan_metadata_url=CKAN_META,
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check7',
            ge_col='Wert',
            ge_min=1,
            ge_max=1000,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'NOx' else False),
                ('Wert', lambda x: True if x else False),  # Exclude NULL values
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check8',
            ge_col='Wert',
            ge_min=0,
            ge_max=200,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'O3' else False),
            ],
        ),
        ExpectColumnMedianToBeBetween(
            task_id='check9',
            ge_col='Wert',
            ge_min=0.15,
            ge_max=2.9,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'CO' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check10',
            ge_col='Wert',
            ge_min=0,
            ge_max=470,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'SO2' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check11',
            ge_col='Wert',
            ge_min=1.1,
            ge_max=280,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'NO2' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check12',
            ge_col='Wert',
            ge_min=0.02,
            ge_max=750,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'NO' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check13',
            ge_col='Wert',
            ge_min=0,
            ge_max=300,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'O3_max_h1' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check14',
            ge_col='Wert',
            ge_min=0,
            ge_max=22,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'O3_nb_h1>120' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check15',
            ge_col='Wert',
            ge_min=0.9,
            ge_max=170,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'PM10' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check16',
            ge_col='Wert',
            ge_min=1.9,
            ge_max=70,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'PM2.5' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check17',
            ge_col='Wert',
            ge_min=0,
            ge_max=80_000,
            ckan_metadata_url=CKAN_META,
            df_apply_func=[
                ('Parameter', lambda x: True if x == 'PN' else False),
            ],
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check18',
            ge_col='Wert',
            ge_min=0,
            ckan_metadata_url=CKAN_META,
        ),
    ]

    clean = CkanCsvDeleteOperator(
        task_id='clean',
        trigger_rule=TriggerRule.ALL_DONE,
        ckan_metadata_url=CKAN_META,
    )

    load >> [*checks] >> clean
