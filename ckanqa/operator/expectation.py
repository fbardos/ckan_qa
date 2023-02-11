import inspect
import logging
from typing import Any, List, Literal, Optional, Tuple

import pandas as pd
from great_expectations.core.expectation_validation_result import \
    ExpectationSuiteValidationResult

import great_expectations as ge
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.context import Context
from ckanqa.connectors import RedisConnector, SftpConnector
from ckanqa.constant import DEFAULT_MONGO_CONN_ID, RESULT_INSERT_COLLECTION
from ckanqa.operator.base import CkanBaseOperator


# TODO: Maybe not used anymore??
class GreatExpectationsBaseOperator(CkanBaseOperator):
    METHOD_NAME: str

    def __init__(
        self,
        ckan_metadata_url: str,
        ge_parameters: dict,
        connector: Literal['sftp', 'redis'] = 'redis',
        source_connection_id: Optional[str] = None,
        mongo_connection_id: str = DEFAULT_MONGO_CONN_ID,
        df_apply_func: Optional[List[Tuple[str, Any]]] = None,
        df_query_str: Optional[str] = None,
        **kwargs
    ):
        super().__init__(ckan_metadata_url=ckan_metadata_url, **kwargs)
        self.source_connection_id = source_connection_id
        self.mongo_connection_id = mongo_connection_id
        self.ge_parameters = ge_parameters
        self.df_apply_func = df_apply_func
        self.df_apply_func_str = self._generate_df_apply_func_string()
        self.df_query_str = df_query_str

        connector_kwargs = dict(connection_id=self.source_connection_id)
        if connector == 'sftp':
            self.connector = SftpConnector(**{k: v for k, v in connector_kwargs.items() if v is not None})
        elif connector == 'redis':
            self.connector = RedisConnector(**{k: v for k, v in connector_kwargs.items() if v is not None})

    def _generate_df_apply_func_string(self):
        if self.df_apply_func is None:
            return
        else:
            res = []
            for _, i in self.df_apply_func:
                res.append(inspect.getsource(i).strip())  # Ugly
            return res

    def filter_df(self, df: pd.DataFrame):
        """Apply filter to DataFrame if set in df_apply_func.

        Multiple apply filters are combined with an logical AND.

        Args:
            df = DataFrame to filter

        """
        df = df.copy()
        if self.df_apply_func:
            for col, filter in self.df_apply_func:
                ser = df[col]
                assert isinstance(ser, pd.Series)
                df['__filter'] = ser.apply(filter)
                df = df[df['__filter']]  # __filter is bool and will filter from original df
                df.drop('__filter', axis=1, inplace=True)
        if self.df_query_str:
            assert isinstance(df, pd.DataFrame)
            df = df.query(self.df_query_str)
        logging.debug(f'EXTRACT from df: \n{df}')
        return df

    def load_csv_as_dataframe(self) -> List[tuple]:
        """Loads files from redis or a directory on NAS via SFTP."""
        container_list = self.connector.load_from_source(self.ckan_id)
        ge_objs = []
        for container in container_list:
            df = self.filter_df(container.dataframe)
            ge_df = ge.from_pandas(df)
            ge_objs.append((container.csv, ge_df))
        if len(ge_objs) == 0:
            raise AttributeError('Returning list "ge_objs" is of length = 0.')
        return ge_objs

    def apply_expectations(self, method_name: str, **kwargs) -> List[Tuple[str, pd.DataFrame, ExpectationSuiteValidationResult]]:
        """Applies expectation from GreatExpectations.

        Returns:
            A list with Tuples (CSV name, loaded dataframe and ExpectationSuiteValidationResult object)
            with the results of the test.

        """
        ge_dfs = self.load_csv_as_dataframe()
        results = []
        for name, df in ge_dfs:
            res = getattr(df, method_name)(**kwargs)
            if res['success']:
                logging.info('=====> SUCCESS: GreatExpectations test was successful.')
            else:
                logging.warning('=====> WARNING: GreatExpectations test was not successful.')
                logging.warning(res)
            results.append((name, df, res))
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
                'ge_query': self.df_query_str,
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


class ExpectColumnDistinctValuesToContainSet(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_distinct_values_to_contain_set'


class ExpectColumnValuesToBeDateutilParseable(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_values_to_be_dateutil_parseable'


class ExpectColumnValuesToBeBetween(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_values_to_be_between'


class ExpectColumnMedianToBeBetween(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_median_to_be_between'


class ExpectColumnStdevToBeBetween(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_stdev_to_be_between'


class ExpectColumnMeanToBeBetween(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_median_to_be_between'


class ExpectColumnValuesToMatchRegexList(GreatExpectationsBaseOperator):
    METHOD_NAME = 'expect_column_values_to_match_regex_list'
