import datetime as dt
import itertools
import os
import sys

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Use relative path for custom modules (easier to handle with airflow deployment atm)
sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../../../ckanqa'))))
from ckanqa.ckan import (CkanPropagateResultMatrix, CkanRedisDeleteOperator,
                         CkanRedisStoreOperator)
from ckanqa.expectation import *
from ckanqa.utils import generate_date_range

CKAN_META = 'https://ckan.opendata.swiss/api/3/action/package_show?id=taglich-aktualisierte-meteodaten-seit-1992'
_year = dt.datetime.now(tz=dt.timezone.utc).year
_CSV_URLS = [
    f'https://data.stadt-zuerich.ch/dataset/ugz_meteodaten_tagesmittelwerte/download/ugz_ogd_meteo_d1_{_year}.csv',
]


with DAG(
    dag_id='ckan_qa_meteodaten',
    schedule_interval='0 3 * * *',
    start_date=dt.datetime(2022, 9, 1),
    catchup=False,
    tags=['ckan', 'swiss'],
) as dag:

    load = CkanRedisStoreOperator(
        task_id='load',
        ckan_metadata_url=CKAN_META,
        extract_csv_urls=_CSV_URLS,
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
                'value_set': ['T', 'RainDur', 'StrGlo', 'T_max_h1', 'p'],
            },
        ),
        ExpectColumnDistinctValuesToBeInSet(
            task_id='check5',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Einheit',
                'value_set': ['°C', 'min', 'hPa', 'W/m2'],
            },
        ),
        ExpectColumnDistinctValuesToBeInSet(
            task_id='check6',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Intervall',
                'value_set': ['d1'],
            },
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check7',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': -5,
                'max_value': 30,
            },
            df_query_str='Parameter == "T"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check8',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0,
                'max_value': 60*24,
            },
            df_query_str='Parameter == "RainDur"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check9',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 891,
                'max_value': 1070,
            },
            df_query_str='Parameter == "p"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check10',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 10,
                'max_value': 400,
            },
            df_query_str='Parameter == "StrGlo"',
        ),
        ExpectColumnValuesToBeBetween(
            task_id='check11',
            ckan_metadata_url=CKAN_META,
            ge_parameters={
                'column': 'Wert',
                'min_value': 0,
                'max_value': 40,
            },
            df_query_str='Parameter == "T_max_h1"',
        ),
        # Checks, whether there is an entry for each day and each "Standort"
        *(ExpectColumnDistinctValuesToContainSet
            .partial(
                task_id='check12',
                ckan_metadata_url=CKAN_META,
                ge_parameters={
                    'column': 'Datum',
                    'value_set': [
                        i.strftime('%Y-%m-%dT%H:%M%z')
                        for i
                        in generate_date_range(
                            dt.datetime(dt.date.today().year, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=1))),
                            dt.datetime.combine(
                                dt.date.today(),
                                dt.datetime.min.time(),
                                tzinfo=dt.timezone(dt.timedelta(hours=1))
                            ) - dt.timedelta(days=1),
                        )
                    ],
                },
            )
            .expand(
                df_query_str=[
                    f'Standort == "{i}"' for i in [
                        'Zch_Rosengartenstrasse', 'Zch_Schimmelstrasse', 'Zch_Stampfenbachstrasse'
                    ]
                ]
            ),
        ),
        # Add df_query_str (filter) for every combination of "Datum" and selected "Parameter",
        # then check standard deviation.
        # Unfortunately, Jinja templating like {{ dag_run.logical }} does not work when outside
        # operator. For this reason, I have to fall back to relative date (no DAG catchup).
        *(ExpectColumnStdevToBeBetween
            .partial(
                task_id='check13',
                ckan_metadata_url=CKAN_META,
                ge_parameters={
                    'column': 'Wert',
                    'min_value': 0,
                    'max_value': 2,
                },
            )
            .expand(
                df_query_str = [
                    f'Datum == "{i.strftime("%Y-%m-%dT%H:%M%z")}" & Parameter == "{j}"'
                    for i, j
                    in itertools.product(generate_date_range(
                        dt.datetime.combine(
                            dt.date.today(),
                            dt.datetime.min.time(),
                            tzinfo=dt.timezone(dt.timedelta(hours=1))
                        ) - dt.timedelta(days=14),
                        dt.datetime.combine(
                            dt.date.today(),
                            dt.datetime.min.time(),
                            tzinfo=dt.timezone(dt.timedelta(hours=1))
                        ) - dt.timedelta(days=1),
                    ), ['T', 'p', 'T_max_h1'])
                ]
            ),
        ),
    ]

    postprocessing = EmptyOperator(task_id='postprocessing')

    clean = CkanRedisDeleteOperator(
        task_id='clean',
        trigger_rule=TriggerRule.ALL_DONE,
        ckan_metadata_url=CKAN_META,
    )

    notify_matrix_all = CkanPropagateResultMatrix(task_id='notify_matrix_all', only_failed=False, short=True)
    notify_matrix_failed = CkanPropagateResultMatrix(task_id='notify_matrix_failed', only_failed=True, short=True)

    load >> [*checks] >> postprocessing >> [clean, notify_matrix_all, notify_matrix_failed]
