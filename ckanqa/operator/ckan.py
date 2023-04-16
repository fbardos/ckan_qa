import io
import logging
import os
import re
from typing import Any, List, Optional, Tuple, Union

import requests
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.ssh.hooks.ssh import SSHHook

from ckanqa.connector import MinioConnector
from ckanqa.context import CkanContext
from ckanqa.hook import CkanDataHook, GreatExpectationsHook
from ckanqa.operator.base import CkanBaseOperator
from ckanqa.utils import mkdir_p


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
        resource_urls = ckan_context.download_urls

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
            bucket_name = Variable.get('CKANQA__S3_BUCKET_NAME_DATA')
            response = requests.get(url)
            filename = re.findall(r'([-a-zA-Z0-9_]+\.[a-zA-Z0-9]+)$', url)[0]
            hook = CkanDataHook(ckan_context.airflow_connection_id, bucket_name=bucket_name)
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
        expected_column_groups: If given, checks, if each subdataframe has all
            the defined combinations as column values. Groups are determined by
            df.groupby(split_by_column_group).size().
            When only one column is grouped, insert a List with values.
            When more than one column is grouped together, add a List with Tuples
            with the corresponding value combinations, for example:
                [('Platz', 'Spur 1'), ('Platz', 'Spur 2'), ('Dorf', 'Spur 1')]

    """
    def __init__(
        self,
        ckan_name: str,
        filelist: Optional[List[str]] = None,
        file_regex: Optional[str] = None,
        split_by_column_group: Optional[Union[str, List[str]]] = None,
        expected_column_groups: List[Any] | List[Tuple[Any, ...]] | None = None,
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.filelist = filelist
        self.file_regex = file_regex
        self.split_by_column_group = split_by_column_group
        self.expected_column_groups = expected_column_groups

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=True)
        bucket_name = Variable.get('CKANQA__S3_BUCKET_NAME_DATA')

        # Extract dag run timestamp from context
        dag_run_timestamp = ckan_context.dag_runtime_iso_8601_basic

        # Iterate over files
        hook = CkanDataHook(ckan_context.airflow_connection_id, bucket_name=bucket_name)
        files_processed = 0
        for path, df in hook.load_dataframes_from_ckan(self.ckan_name, dag_run_timestamp):

            # If path not in filelist, then skip the processing of this file.
            if self.filelist:
                if not any([i in path for i in self.filelist]):
                    continue

            # If filter regex is set for files, check and skip when regex does not match current path
            if self.file_regex:
                if not re.match(self.file_regex, path):
                    continue

            if self.split_by_column_group:
                if self.expected_column_groups:
                    value_counts = df.groupby(self.split_by_column_group).size().index.tolist()
                    try:
                        assert (actual := set(value_counts)) == (target := set(self.expected_column_groups))
                    except AssertionError as err:
                        raise Exception(
                            'Group values do not match expectation:'
                            f' IS: {actual}, \nSHOULD: {target}'
                        ) from err

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
            files_processed += 1
        if files_processed == 0:
            raise Exception(f'No files were processed. File list: {self.filelist}, File regex: {self.file_regex}.')


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
        bucket_name = Variable.get('CKANQA__S3_BUCKET_NAME_DATA')

        # First, list files, then delete them after filter...
        hook = CkanDataHook(ckan_context.airflow_connection_id, bucket_name=bucket_name)

        # Step 1: Retrieve all available files.
        list_files = hook.connector.list_path_objects(self.get_s3_prefix(context))

        # Step 2, delete csv
        csvs = [i for i in list_files if i.endswith('.csv')]
        hook.delete_files(csvs)

        # Step 3, delete parquet, if keep_when_failed = True, skip
        parquets = [i for i in list_files if i.endswith('.parquet')]
        if all(ckan_context.checkpoint_success):
            pass
        elif not all(ckan_context.checkpoint_success):
            if self.keep_when_failed:
                raise AirflowSkipException
        else:
            raise ValueError('CkanContext.checkpoint_success was neither true nor false.')
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


class PublishDataDocsOperator(CkanBaseOperator):

    def __init__(
        self,
        ckan_name: str,
        clear_before_copy: bool = True,
        entry_directory_name: str = 'data_docs',
        **kwargs
    ):
        super().__init__(ckan_name=ckan_name, **kwargs)
        self.clear_before_copy = clear_before_copy
        self.entry_directory_name = entry_directory_name

    def execute(self, context):

        # Load Airflow variables
        connection_id = Variable.get('CKANQA__S3_CONN_ID')
        bucket_name = Variable.get('CKANQA__S3_BUCKET_NAME_META')
        destination_path = Variable.get('CKANQA__DATA_DOCS_PATH')
        sftp_connection_id = Variable.get('CKANQA__SFTP_CONN_ID')

        # Load files from S3
        s3_hook = GreatExpectationsHook(connection_id=connection_id, bucket_name=bucket_name)

        # Iterate over all files
        sftp_hook = SFTPHook(sftp_connection_id)
        with sftp_hook.get_conn() as sftp_client:

            # Delete existing files if clear_before_copy == True
            if self.clear_before_copy:
                ssh_hook = SSHHook(sftp_connection_id)
                with ssh_hook.get_conn() as ssh_client:
                    assert destination_path is not None and self.entry_directory_name is not None
                    ssh_client.exec_command(f'rm -rf {destination_path}/{self.entry_directory_name}')

            # Currently, cannot use S3Hook method to retrieve file objects. Instead, use
            # s3_hook.connector.hook.get_conn() to get botocore.client.S3.
            # Reason:
            #   Inside /site-packages/airflow/providers/amazon/aws/hooks/s3.py
            #   --> Content gets decoded with UTF-8. This does not work for e.g. binary files.
            #       For example, 'data_docs/static/fonts/HKGrotesk/HKGrotesk-Bold.otf' is a binary file.
            # As a workaround, we use the boto.s3.Client itself.
            assert isinstance(s3_hook.connector, MinioConnector)
            bucket = s3_hook.connector.hook.get_bucket(bucket_name)
            for obj in bucket.objects.filter(Prefix=self.entry_directory_name):
                buffer = io.BytesIO()
                path, filename = obj.key.rsplit('/', 1)
                bucket.download_fileobj(obj.key, buffer)
                buffer.seek(0)
                mkdir_p(sftp_client, destination_path + path)
                sftp_client.putfo(buffer, filename)
