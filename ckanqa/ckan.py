import sys
import datetime as dt
import io
import json
import os
import re
import pprint
import logging
import asyncio
from typing import List, Optional
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection

import requests

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.email import send_email_smtp
from airflow.models.baseoperator import BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.context import Context
from airflow.models import Variable
from sqlalchemy.sql.sqltypes import Boolean

from ckanqa.matrix_hook import MatrixHook

sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../ckanqa'))))
from ckanqa.constant import DEFAULT_MONGO_CONN_ID, RESULT_INSERT_COLLECTION, DEFAULT_SFTP_CONN_ID, DEFAULT_MATRIX_CONN_ID, MATRIX_ROOM_ID_ALL, MATRIX_ROOM_ID_FAILURE, DEFAULT_REDIS_CONN_ID, REDIS_DEFAULT_TTL


class ResultsExtractor:
    """Loads data from GreatExectation results.

        - Connects to MongoDB for results.
        - Generates human-readable results (string).

    """

    def __init__(
        self,
        mongo_connection_id: str = DEFAULT_MONGO_CONN_ID,
        short: bool = False,
    ):
        self.mongo_connection_id = mongo_connection_id
        self._results = None
        self._short = short

    @property
    def results(self):
        if self._results is None:
            raise ValueError('Results is currently not set.')
        else:
            return self._results

    def load_results_mongo(self, context: Context, only_failed: bool = False):
        if self._results is None:
            with MongoHook(self.mongo_connection_id) as client:
                query = {
                        'airflow_dag': context.get('dag').dag_id,
                        'airflow_execdate': context.get('dag_run').execution_date,
                }
                if only_failed:
                    query['ge_success'] = False
                results = client.find(mongo_collection=RESULT_INSERT_COLLECTION, query=query)
            self._results = [i for i in results]
        return self._results

    def _generate_summary_header(self, results: list) -> List[str]:
        lines = []
        lines.append(
            '================================ TEST SUMMARY ================================'
        )
        lines.append(f'RESULT\t\t{"x" if len([i for i in results if i["ge_success"] == False]) > 0 else "✓"}')
        lines.append('')
        lines.append(f'CKAN NAME\t{results[0]["ckan_name"]}')
        lines.append(f'CKAN ID\t\t{results[0]["ckan_id"]}')
        lines.append(f'AIRFLOW DAG\t{results[0]["airflow_dag"]}')
        lines.append(f'AIRFLOW RUN\t{results[0]["airflow_run"]}')
        lines.append(f'AIRFLOW EXC\t{results[0]["airflow_execdate"]}')
        return lines

    def _generate_summary(self, results: list) -> List[str]:
        """Generates list of lines for printing summary.

        Maybe, there is a cleaner way to generate the lines for printing.

        """
        lines = self._generate_summary_header(results)
        lines.append('')

        for file in {i['ckan_file'] for i in results}:
            lines.append(f'  FILE {file}')
            for check in (i for i in results if i['ckan_file'] == file):
                lines.append(f'\t{"✓" if check["ge_result"]["success"] else "x"} CHECK {check["ge_expectation"]}')
                res = pprint.pformat(str(check['ge_params']), compact=True).replace('\n', '\n\t  ')
                lines.append('\t  ' + res)
                if check.get('ge_filter'):
                    for filter in check['ge_filter']:
                        lines.append('\t  FILTER ' + filter)
            lines.append('')

        return lines

    def _generate_errors(self, results: list) -> List[str]:
        lines = []
        lines.append('')
        lines.append(
            '================================ TEST ERRORS ================================'
        )
        lines.append('')
        for file in {i['ckan_file'] for i in results}:
            lines.append(f'  FILE {file}')
            for check in (i for i in results if i['ckan_file'] == file and i['ge_success'] == False):
                lines.append('\t --------------------------------------')
                res = pprint.pformat(check['ge_result']).replace('\n', '\n\t|')
                res = '\t|' + res
                lines.append(res)
            lines.append('')
        return lines

    def generate_message_string(self, context: Context):
        """

        Example output:

        ================================ SUMMARY ================================
        CKAN NAME   temparatur-grundwasser
        CKAN ID     ae23252a-54b8-46f3-9841-9c8053bdc5ca

            FILE 2022-09-05-messwerte_tiefenbrunnen_2022.csv
              ✓ CHECK expect_table_columns_to_match_ordered_list
                    PARAMS column=['column1', 'column2', 'column3', 'column4']
              x CHECK expect_column_values_to_be_between
                    PARAMS column='humidity', min=0, max=30

            FILE 2022-09-05-messwerte_tiefenbrunnen_2021.csv
              ✓ CHECK expect_table_columns_to_match_ordered_list
                    PARAMS column=['column1', 'column2', 'column3', 'column4']
              x CHECK expect_column_values_to_be_between
                    PARAMS column='humidity', min=0, max=30

        ================================ ERRORS ================================
        (...)

        """
        results = self.load_results_mongo(context)
        if self._short:
            lines = self._generate_summary_header(results)
        else:
            summary = self._generate_summary(results)
            if len([i for i in results if i['ge_success'] == False]) > 0:
                errors = self._generate_errors(results)
                lines = [*summary, *errors]
            else:
                lines = [*summary]
        return '\n'.join(lines)


class CkanBaseOperator(BaseOperator):

    def __init__(self, ckan_metadata_url: str, store_path: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.ckan_metadata_url = ckan_metadata_url
        self.store_path = store_path

        # Will only be loaded when executing, not DagBag loading.
        self._meta = None
        self.ckan_name = None
        self.ckan_id = None

    @property
    def remote_filepath(self):
        if self.ckan_id is None:
            self._set_metadata()
        if self.store_path is None:
            raise AttributeError('Attribute store_path is not set.')
        return os.path.join(self.store_path, self.ckan_id)

    def _set_metadata(self):
        hook = RedisHook(DEFAULT_REDIS_CONN_ID)
        with hook.get_conn() as conn:
            key = 'ckan:meta:customid'
            if conn.exists(key):
                logging.debug('Found cached response in redis, load from there.')
                r = conn.get(key)
                if r is None:
                    raise ValueError('Redis key not found.')
                r_json = r.decode('utf-8')
                logging.debug(f'Extracted str from redis (extract): {r_json[:100]}')
            else:
                logging.debug('Not found cached response in redis, load from CKAN API.')
                r = requests.get(self.ckan_metadata_url)
                r_json = r.text
                conn.set(key, r_json, ex=REDIS_DEFAULT_TTL)
        logging.debug(f'Extracted meta (extract): {r_json[:80]}')
        self._meta = json.loads(r_json)
        self.ckan_name = self._meta['result']['name']
        self.ckan_id = self._meta['result']['id']


class CkanCsvStoreOperator(CkanBaseOperator):
    """Stores CSVs on remote location via SFTP.

    This operator will create a directory (named after CKAN ID) and store
    all CSVs in this directory, under the specified store_path.

    Args:
        ckan_metadata_url: URL to CKAN package metadata.
        extract_csv_urls: List of URLs to CSVs to extract and store.
            If not set, the operator will download all CSVs from CKAN package.
        sftp_connection_id: Connection ID for SFTP, used by Airflow.
        store_path: Path to store on destination (SFTP).
        **kwargs: Kwargs for Airflow task context.

    """

    def __init__(
        self,
        ckan_metadata_url: str,
        extract_csv_urls: Optional[List[str]] = None,
        sftp_connection_id: str = DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        **kwargs
    ):
        super().__init__(ckan_metadata_url=ckan_metadata_url, store_path=store_path, **kwargs)
        self.extract_csv_urls = extract_csv_urls
        self.sftp_connection_id = sftp_connection_id

    @property
    def remote_filepath(self):
        if self._meta is None:
            self._set_metadata()
        return os.path.join(self.store_path, self.ckan_id)

    def execute(self, context):
        if self._meta is None:
            self._set_metadata()

        if self.extract_csv_urls is None:
            csv_urls = [i['download_url'] for i in self._meta['result']['resources'] if i['media_type'] == 'text/csv']
        else:
            csv_urls = self.extract_csv_urls

        # Retrieve CSVs and store to NAS
        sftp_client = SFTPHook(self.sftp_connection_id)
        conn = sftp_client.get_conn()
        try:
            try:
                conn.chdir(self.remote_filepath)
            except IOError:
                conn.mkdir(self.remote_filepath)
                conn.chdir(self.remote_filepath)
            for csv in csv_urls:
                r = requests.get(csv)
                original_filename = re.findall(r'([-a-zA-Z0-9_]+)\.csv', csv)[0]
                conn.putfo(io.BytesIO(r.content), f'{original_filename}.csv')
        finally:
            sftp_client.close_conn()


class CkanCsvDeleteOperator(CkanBaseOperator):
    """Deletes stored files on remote location via SFTP.

    This operator will delete all files on remote location's path,
    and the directory (named after CKAN ID) itself.

    Args:
        ckan_metadata_url: URL to CKAN package metadata.
        sftp_connection_id: Connection ID for SFTP, used by Airflow.
        store_path: Path to store on destination (SFTP).
        **kwargs: Kwargs for Airflow task context.

    """

    def __init__(
        self,
        ckan_metadata_url: str,
        sftp_connection_id: str = DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        **kwargs
    ):
        super().__init__(ckan_metadata_url=ckan_metadata_url, store_path=store_path, **kwargs)
        self.ckan_metadata_url = ckan_metadata_url
        self.sftp_connection_id = sftp_connection_id

    @property
    def remote_filepath(self):
        if self._meta is None:
            self._set_metadata()
        return os.path.join(self.store_path, self.ckan_id)

    def execute(self, context):
        if self._meta is None:
            self._set_metadata()

        # Delete filese in directory on NAS
        sftp_client = SFTPHook(self.sftp_connection_id)
        conn = sftp_client.get_conn()
        try:
            conn.chdir(self.store_path)

            # Delete files, than the directory itself.
            files = conn.listdir(self.ckan_id)
            for file in files:
                conn.remove(os.path.join(self.ckan_id, file))
            conn.rmdir(self.ckan_id)
        finally:
            sftp_client.close_conn()


class CkanPropagateResultMatrix(BaseOperator):
    """ Builds output string and pushes message to matrix (Fediverse).

    Args:
        mongo_connection_id: Connection ID for MongoDB with stored results
        matrix_connection_id: Connection ID for matrix messages
        only_failed: If True, only DAG runs with at least one failed check will be sent to Matrix
        short: If true, only the summary will be sent
        **kwargs: Kwargs for Airflow task context.

    """
    def __init__(
        self,
        mongo_connection_id: str = DEFAULT_MONGO_CONN_ID,
        matrix_connection_id: str = DEFAULT_MATRIX_CONN_ID,
        only_failed: bool = False,
        short: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.mongo_connection_id = mongo_connection_id
        self.matrix_connection_id = matrix_connection_id
        self._only_failed = only_failed
        self._short = short
        if self._short:
            self.extractor = ResultsExtractor(self.mongo_connection_id, short=True)
        else:
            self.extractor = ResultsExtractor(self.mongo_connection_id)


    def execute(self, context):

        # First, count amount of results. If no results, then no test has failed.
        results = self.extractor.load_results_mongo(context, only_failed=self._only_failed)

        if len(results) > 0:
            msg = self.extractor.generate_message_string(context)
            matrix = MatrixHook(DEFAULT_MATRIX_CONN_ID)

            # Retrieve room ID
            if self._only_failed:
                room_alias = Variable.get(MATRIX_ROOM_ID_FAILURE)
            else:
                room_alias = Variable.get(MATRIX_ROOM_ID_ALL)
            room_id = asyncio.run(matrix.resolve_alias(room_alias))

            asyncio.run(matrix.send_markdown_message(room_id, msg))

