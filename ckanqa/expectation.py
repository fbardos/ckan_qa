import inspect
import io
import json
import logging
import os
import sys
from abc import ABC
from multiprocessing import Value
from typing import List, Optional, Tuple

import great_expectations as ge
import pandas as pd
import requests
from great_expectations.core.expectation_validation_result import \
    ExpectationSuiteValidationResult

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.context import Context

# Use relative path for custom modules (easier to handle with airflow deployment atm)
sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../ckanqa'))))
from ckanqa.constant import (DEFAULT_MONGO_CONN_ID, DEFAULT_SFTP_CONN_ID,
                             RESULT_INSERT_COLLECTION)
from ckanqa.ckan import CkanBaseOperator


class ExpectationMixin(ABC):
    sftp_connection_id: str
    remote_filepath: str
    df_apply_func: Optional[List[Tuple[str, object]]]

    def filter_df(self, df: pd.DataFrame):
        """Apply filter to DataFrame if set in df_apply_func.

        Multiple apply filters are combined with an logical AND.

        Args:
            df = DataFrame to filter

        """
        df = df.copy()
        if self.df_apply_func:
            for filter in self.df_apply_func:
                df['__filter']= df[filter[0]].apply(filter[1])
                df = df[df['__filter']]
                df.drop('__filter', axis=1, inplace=True)
        logging.debug(f'EXTRACT from df: \n{df}')
        return df

    def load_csv_as_dataframe_from_nas(self) -> list:
        """Loads files from a directory on NAS via SFTP."""
        sftp_client = SFTPHook(self.sftp_connection_id)
        conn = sftp_client.get_conn()
        try:
            conn.chdir(self.remote_filepath)
            ge_objs = []
            for csv in conn.listdir():
                logging.info(f'Loading {csv}...')
                with io.BytesIO() as fl:

                    # Retrieve CSV with SFTP
                    conn.getfo(csv, fl)
                    fl.seek(0)
                    df = pd.read_csv(fl)
                    df = self.filter_df(df)
                ge_objs.append((csv, ge.from_pandas(df)))
        finally:
            sftp_client.close_conn()
        return ge_objs

    def apply_expectations(self, method_name: str, **kwargs) -> List[Tuple[str, pd.DataFrame, ExpectationSuiteValidationResult]]:
        """Applies expectation from GreatExpectations.

        Returns:
            A list with Tuples (CSV name, loaded dataframe and ExpectationSuiteValidationResult object)
            with the results of the test.

        """
        ge_dfs = self.load_csv_as_dataframe_from_nas()
        results = []
        for df in ge_dfs:
            res = getattr(df[1], method_name)(**kwargs)
            if res['success']:
                logging.info('=====> SUCCESS: GreatExpectations test was successful.')
            else:
                logging.warning('=====> WARNING: GreatExpectations test was not successful.')
                logging.warning(res)
            results.append((df[0], df[1], res))
        return results

    def log_results(self, results: List[Tuple[str, pd.DataFrame, ExpectationSuiteValidationResult]]):
        results_with_error = []
        for result in results:
            _, _, ge_result = result
            if ge_result.success:
                pass
            else:
                results_with_error.append(result)
        if len(results_with_error) == 0:
            logging.info('GreatExpectations run SUCCESSFUL, without missed expectations.')
        else:
            logging.warning(f'FAILED: {len(results_with_error)} out of {len(results)} tested datasets have failed.')
        return results_with_error



class GreatExpectationsBaseOperator(CkanBaseOperator, ExpectationMixin):
    METHOD_NAME: str

    def __init__(
        self,
        ckan_metadata_url: str,
        ge_parameters: dict,
        sftp_connection_id: str = DEFAULT_SFTP_CONN_ID,
        mongo_connection_id: str = DEFAULT_MONGO_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        df_apply_func: Optional[List[Tuple[str, object]]] = None,
        **kwargs
    ):
        super().__init__(ckan_metadata_url=ckan_metadata_url, store_path=store_path, **kwargs)
        self.sftp_connection_id = sftp_connection_id
        self.mongo_connection_id = mongo_connection_id
        self.ge_parameters = ge_parameters
        self.df_apply_func = df_apply_func
        self.df_apply_func_str = self._generate_df_apply_func_string()

    def _generate_df_apply_func_string(self):
        if self.df_apply_func is None:
            return
        else:
            res = []
            for _, i in self.df_apply_func:
                res.append(inspect.getsource(i).strip())  # Not very pretty...
            return res

    def store_results_mongodb(self, context: Context, results: List[Tuple[str, pd.DataFrame, ExpectationSuiteValidationResult]]):
        insert_dicts = []
        for csv, _, ge_result in results:
            insert_dicts.append({
                'ckan_name': self.ckan_name,
                'ckan_id': self.ckan_id,
                'ckan_file': csv,
                'airflow_dag': context.get('dag').dag_id,
                'airflow_run': context.get('run_id'),
                'airflow_execdate': context.get('dag_run').execution_date,
                'airflow_task': context.get('task').task_id,
                'ge_expectation': self.METHOD_NAME,
                'ge_success': ge_result.success,
                'ge_params': self.ge_parameters,
                'ge_filter': self.df_apply_func_str,
                'ge_result': ge_result.to_raw_dict(),
            })

        with MongoHook(self.mongo_connection_id) as client:
            client.insert_many(RESULT_INSERT_COLLECTION, insert_dicts)

    def execute(self, context):
        if self._meta is None:
            self._set_metadata()
        results = self.apply_expectations(self.METHOD_NAME, **self.ge_parameters)
        self.store_results_mongodb(context, results)


class ExpectTableColumnsToMatchOrderedListOperator(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_table_columns_to_match_ordered_list'


class ExpectColumnDistinctValuesToBeInSet(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_distinct_values_to_be_in_set'


class ExpectColumnValuesToBeDateutilParseable(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_values_to_be_dateutil_parseable'


class ExpectColumnValuesToBeBetween(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_values_to_be_between'


class ExpectColumnMedianToBeBetween(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_median_to_be_between'


class ExpectColumnMeanToBeBetween(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_median_to_be_between'


class ExpectColumnValuesToMatchRegexList(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_values_to_match_regex_list'
