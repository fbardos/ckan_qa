import io
import logging
import pprint
import re
from typing import List, Optional, Union

import requests

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.context import Context
from ckanqa.constant import (DEFAULT_MONGO_CONN_ID,
                             RESULT_INSERT_COLLECTION)
from ckanqa.context import CkanContext
from ckanqa.hook import CkanDataHook
from ckanqa.operator.base import CkanBaseOperator


# TODO: Currently not needed. Refactor or delete.
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
            for check in (i for i in results if i['ckan_file'] == file and not i['ge_success']):
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
            if len([i for i in results if not i['ge_success']]) > 0:
                errors = self._generate_errors(results)
                lines = [*summary, *errors]
            else:
                lines = [*summary]
        return '\n'.join(lines)


class CkanExtractOperator(CkanBaseOperator):
    """Extracts datasets from CKAN and stores them in the original format on Minio.

    Given a CKAN meta URL, it extracts the relevant resources.

    Args:
        ckan_metadata_url: URL to CKAN metadata.
        connector:
        connection_id: Name of the stored Airlfow connection for storage.
        filelist: List of filenames to extract. If not set, takes all files from metadata.
        file_regex: Filters files to extract from given regex.


    Example:

        >>> t1 = CkanExtractOperator(
        >>>     task_id='extract_ckan',
        >>>     ckan_metadata_url=CKAN_META,
        >>>     file_regex=r'.*\.csv$'
        >>> )


    """
    def __init__(
        self,
        ckan_name: str,
        filelist: Optional[List[str]] = None,
        file_regex: Optional[str] = None,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.filelist = filelist
        self.file_regex = file_regex

        if self.filelist and self.file_regex:
            raise ValueError('Cannot filter by filelist AND regex. Choose one.')

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)

        # Gather all resources
        # TODO: Maybe move to CkanInstanceHook?
        resource_urls = [i['download_url'] for i in ckan_context.ckan_metadata['result']['resources']]

        # Filter resources
        if self.filelist:
            urls_filtered = [i for i in resource_urls if i in self.filelist]
        elif self.file_regex:
            regex = re.compile(self.file_regex)
            urls_filtered = list(filter(regex.match, resource_urls))
        else:
            urls_filtered = resource_urls

        if len(urls_filtered) == 0:
            logging.warning('The current file filter will not download any resources.')
        else:
            logging.info(f'Now downloading {len(urls_filtered)} resource(s).')

        # Extract dag run timestamp from context
        dag_run_timestamp = ckan_context.dag_runtime_iso_8601_basic

        # Download all resources and store them with hook
        for url in urls_filtered:
            response = requests.get(url)
            filename = re.findall(r'([-a-zA-Z0-9_]+\.[a-zA-Z0-9]+)$', url)[0]
            hook = CkanDataHook(ckan_context.airflow_connection_id)
            hook.write_from_request(self.ckan_name, dag_run_timestamp, filename, response)


class CkanParquetOperator(CkanBaseOperator):
    """Converts extracted CSVs into a parquet file.

    Transforms all CSVs in a given directory into parquet.
    Connector is used for load and save.

    Args:
        ckan_metadata_url: URL to CKAN metadata.
        connector:
        connection_id: Name of the stored Airlfow connection for storage.
        split_by_column_group: If given, creates one separate .parquet file
            for each group column. E.g. if column `parameter` contains
            distinct values `CO2` and `O3`, then it will create
            two .parquet files per single .csv.

    """
    # TODO: Maybe add a parameter with expected split_by_column_group values?
    def __init__(
        self,
        ckan_name: str,
        filelist: Optional[List[str]] = None,  # TODO: Currently not implemented
        file_regex: Optional[str] = None,  # TODO: Currently not implemented
        split_by_column_group: Optional[Union[str, List[str]]] = None,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.filelist = filelist
        self.file_regex = file_regex
        self.split_by_column_group = split_by_column_group

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)

        # Extract dag run timestamp from context
        dag_run_timestamp = ckan_context.dag_runtime_iso_8601_basic

        # Iterate over files
        hook = CkanDataHook(ckan_context.airflow_connection_id)
        for path, df in hook.load_dataframe_from_filetype(self.ckan_name, dag_run_timestamp, '.csv'):
            if self.split_by_column_group:
                sub_dfs = [(group, sdf) for group, sdf in df.groupby(self.split_by_column_group)]
                for group_values, sub_df in sub_dfs:
                    filename = re.sub(r'.*/([^/]+)\.csv', r'\1.parquet', path)
                    if isinstance(group_values, str):
                        group_values = (group_values, )
                    assert isinstance(group_values, tuple)
                    for group_value in group_values:
                        filename = re.sub(r'(.*)(\.parquet)', r'\1__' + group_value + r'\2', filename)
                    buffer = io.BytesIO()
                    sub_df.to_parquet(buffer)
                    hook.write_from_buffer(self.ckan_name, dag_run_timestamp, filename, buffer)
            else:
                filename = re.sub(r'.*/([^/]+)\.csv', r'\1.parquet', path)
                buffer = io.BytesIO()
                df.to_parquet(buffer)
                hook.write_from_buffer(self.ckan_name, dag_run_timestamp, filename, buffer)


class CkanDeleteOperator(CkanBaseOperator):
    """Deletes old files, when all corresponding GE expectations were met.

    """

    def __init__(
        self,
        ckan_name: str,
        keep_when_failed: bool = True,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.keep_when_failed = keep_when_failed

    def execute(self, context):
        """

        Gets data from CkanContext to evaluate if a checkpoint has failed.

        """
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)

        # First, list files, then delete them after filter...
        hook = CkanDataHook(ckan_context.airflow_connection_id)

        # Step 1: Retrieve all available files.
        dag_run_iso_8601 = ckan_context.dag_runtime_iso_8601_basic
        # TODO: DRY: path joining is already done when storing csv... use a method
        #   which persistently retruns the full prefix.
        list_files = hook.list_files('/'.join([self.ckan_name, dag_run_iso_8601]))

        # Step 2, delete csv
        csvs = [i for i in list_files if i.endswith('.csv')]
        hook.delete_files(csvs)

        # Step 3, delete parquet, if keep_when_failed = True, skip
        parquets = [i for i in list_files if i.endswith('.parquet')]
        if ckan_context.checkpoint_success:
            pass
        elif not ckan_context.checkpoint_success:
            if self.keep_when_failed:
                parquets = []
            else:
                pass
        else:
            raise ValueError('CkanContext.checkpoint_success was neither true nor false.')

        if len(parquets) > 0:
            hook.delete_files(parquets)


class CkanDeleteContextOperator(CkanBaseOperator):
    """Delete persistet CkanContext from Redis """

    def __init__(
        self,
        ckan_name: str,
        keep_when_failed: bool = True,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.keep_when_failed = keep_when_failed

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        ckan_context.delete_context_redis()


# TODO: Currently not needed?
# class CkanStoreOperator(CkanBaseOperator):
    # connector: BaseConnector

    # def __init__(
        # self,
        # ckan_metadata_url: str,
        # connector: BaseConnector,
        # connection_id: str,
        # extract_csv_urls: Optional[List[str]] = None,
        # csv_pattern: Optional[str] = None,
        # **kwargs
    # ):
        # super().__init__(ckan_metadata_url=ckan_metadata_url, **kwargs)
        # self.connector = connector
        # self.connection_id = connection_id
        # self.extract_csv_urls = extract_csv_urls
        # self.csv_pattern = csv_pattern

    # def execute(self, context):
        # assert self.meta is not None
        # if self.extract_csv_urls:
            # csv_urls = self.extract_csv_urls
        # elif self.csv_pattern:
            # """Using a regex pattern on filename.
            # Supported format keywords:
                # Y (year), m (month), d (day), H (hour), M (minute)
            # Same format codes as in https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
            # Don't forget to escape curly braces from Regex quantifiers with {{}}.
            # """
            # exec_date = context.get('dag_run').execution_date
            # assert exec_date is not None
            # pat = self.csv_pattern.format(
                # Y=exec_date.strftime('%Y'),
                # m=exec_date.strftime('%m'),
                # d=exec_date.strftime('%d'),
                # H=exec_date.strftime('%H'),
                # M=exec_date.strftime('%M'),
            # )
            # csv_urls = [
                # i['download_url'] for i in self.meta['result']['resources'] if bool(re.search(pat, i['download_url']))
            # ]
        # else:
            # csv_urls = [i['download_url'] for i in self.meta['result']['resources'] if i['media_type'] == 'text/csv']
        # for csv in csv_urls:
            # r = requests.get(csv)
            # filename = re.findall(r'([-a-zA-Z0-9_]+)\.csv', csv)[0]
            # self.connector.write_to_source(self.ckan_id, filename, r)


# class CkanSftpStoreOperator(CkanStoreOperator):

    # def __init__(
        # self,
        # ckan_metadata_url: str,
        # connector: BaseConnector,
        # extract_csv_urls: Optional[List[str]] = None,
        # csv_pattern: Optional[str] = None,
        # connection_id: str = DEFAULT_SFTP_CONN_ID,
        # **kwargs
    # ):
        # super().__init__(
            # ckan_metadata_url=ckan_metadata_url,
            # connector=connector,
            # connection_id=connection_id,
            # extract_csv_urls=extract_csv_urls,
            # csv_pattern=csv_pattern,
            # **kwargs
        # )


# class CkanRedisStoreOperator(CkanStoreOperator):

    # def __init__(
        # self,
        # ckan_metadata_url: str,
        # connector: BaseConnector,
        # extract_csv_urls: Optional[List[str]] = None,
        # csv_pattern: Optional[str] = None,
        # connection_id: str = DEFAULT_REDIS_CONN_ID,
        # **kwargs
    # ):
        # super().__init__(
            # ckan_metadata_url=ckan_metadata_url,
            # connector=connector,
            # connection_id=connection_id,
            # extract_csv_urls=extract_csv_urls,
            # csv_pattern=csv_pattern,
            # **kwargs
        # )


# class CkanDeleteOperator(CkanBaseOperator):

    # def __init__(self, ckan_metadata_url: str, connector: BaseConnector, **kwargs):
        # super().__init__(ckan_metadata_url=ckan_metadata_url, **kwargs)
        # self.connector = connector

    # def execute(self, context):
        # self.connector.delete_from_source(self.ckan_id)


# class CkanSftpDeleteOperator(CkanDeleteOperator):

    # def __init__(
        # self,
        # ckan_metadata_url: str,
        # connector: BaseConnector,
        # **kwargs
    # ):
        # super().__init__(ckan_metadata_url=ckan_metadata_url, connector=connector, **kwargs)


# class CkanRedisDeleteOperator(CkanDeleteOperator):

    # def __init__(
        # self,
        # ckan_metadata_url: str,
        # connector: BaseConnector,
        # **kwargs
    # ):
        # super().__init__(ckan_metadata_url=ckan_metadata_url, connector=connector, **kwargs)


# class CkanAbstractOperatorFactory(ABC):

    # def __init__(self, ckan_metadata_url: str, connection_id: str):
        # self.ckan_metadata_url = ckan_metadata_url
        # self.connection_id = connection_id

    # @abstractmethod
    # def create_store_operator(self) -> CkanStoreOperator:
        # pass

    # @abstractmethod
    # def create_delete_operator(self) -> CkanDeleteOperator:
        # pass


# class CkanSftpOperatorFactory(CkanAbstractOperatorFactory):
    # """Additional possible in **kwargs: sftp_store_path for custom store paths."""

    # def __init__(
        # self,
        # ckan_metadata_url: str,
        # connection_id: str = DEFAULT_SFTP_CONN_ID,
        # store_path: str = os.path.join('data', 'ckan')
    # ):
        # super().__init__(ckan_metadata_url, connection_id)
        # self.connector = SftpConnector(connection_id=connection_id, store_path=store_path)

    # def create_store_operator(
        # self,
        # extract_csv_urls: Optional[List[str]] = None,
        # csv_pattern: Optional[str] = None,
        # connection_id: str = DEFAULT_REDIS_CONN_ID,
        # **kwargs
    # ) -> CkanStoreOperator:
        # return CkanSftpStoreOperator(
            # ckan_metadata_url=self.ckan_metadata_url,
            # connector=self.connector,
            # extract_csv_urls=extract_csv_urls,
            # csv_pattern=csv_pattern,
            # connection_id=connection_id,
            # **kwargs
        # )

    # def create_delete_operator(self, **kwargs) -> CkanDeleteOperator:
        # return CkanSftpDeleteOperator(
            # ckan_metadata_url=self.ckan_metadata_url,
            # connector=self.connector,
            # **kwargs
        # )


# class CkanRedisOperatorFactory(CkanAbstractOperatorFactory):

    # def __init__(self, ckan_metadata_url: str, connection_id: str = DEFAULT_REDIS_CONN_ID):
        # super().__init__(ckan_metadata_url, connection_id)
        # self.connector = RedisConnector(connection_id)

    # def create_store_operator(
        # self,
        # extract_csv_urls: Optional[List[str]] = None,
        # csv_pattern: Optional[str] = None,
        # connection_id: str = DEFAULT_REDIS_CONN_ID,
        # **kwargs
    # ) -> CkanStoreOperator:
        # return CkanRedisStoreOperator(
            # ckan_metadata_url=self.ckan_metadata_url,
            # connector=self.connector,
            # extract_csv_urls=extract_csv_urls,
            # csv_pattern=csv_pattern,
            # connection_id=connection_id,
            # **kwargs
        # )

    # def create_delete_operator(self, **kwargs) -> CkanDeleteOperator:
        # return CkanRedisDeleteOperator(
            # ckan_metadata_url=self.ckan_metadata_url,
            # connector=self.connector,
            # **kwargs
        # )


# class CkanPropagateResultMatrix(BaseOperator):
    # """ Builds output string and pushes message to matrix (Fediverse).

    # Args:
        # mongo_connection_id: Connection ID for MongoDB with stored results
        # matrix_connection_id: Connection ID for matrix messages
        # only_failed: If True, only DAG runs with at least one failed check will be sent to Matrix
        # short: If true, only the summary will be sent
        # **kwargs: Kwargs for Airflow task context.

    # """
    # def __init__(
        # self,
        # mongo_connection_id: str = DEFAULT_MONGO_CONN_ID,
        # matrix_connection_id: str = DEFAULT_MATRIX_CONN_ID,
        # only_failed: bool = False,
        # short: bool = False,
        # **kwargs
    # ):
        # super().__init__(**kwargs)
        # self.mongo_connection_id = mongo_connection_id
        # self.matrix_connection_id = matrix_connection_id
        # self._only_failed = only_failed
        # self._short = short
        # if self._short:
            # self.extractor = ResultsExtractor(self.mongo_connection_id, short=True)
        # else:
            # self.extractor = ResultsExtractor(self.mongo_connection_id)

    # def execute(self, context):

        # # First, count amount of results. If no results, then no test has failed.
        # results = self.extractor.load_results_mongo(context, only_failed=self._only_failed)

        # if len(results) > 0:
            # msg = self.extractor.generate_message_string(context)
            # matrix = MatrixHook(DEFAULT_MATRIX_CONN_ID)

            # # Retrieve room ID
            # if self._only_failed:
                # room_alias = Variable.get(MATRIX_ROOM_ID_FAILURE)
            # else:
                # room_alias = Variable.get(MATRIX_ROOM_ID_ALL)
            # room_id = asyncio.run(matrix.resolve_alias(room_alias))

            # asyncio.run(matrix.send_markdown_message(room_id, msg))
