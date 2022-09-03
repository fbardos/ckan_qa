import io
import json
import logging
import os
from abc import ABC
from typing import List, Optional, Tuple

import great_expectations as ge
import pandas as pd
import requests

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

_DEFAULT_SFTP_CONN_ID = 'sftp_bucket'


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

    def apply_expectations(self, method_name: str, **kwargs):
        """Applies expectation from GreatExpectations."""
        ge_dfs = self.load_csv_as_dataframe_from_nas()
        results = []
        for df in ge_dfs:
            res = getattr(df[1], method_name)(**kwargs)
            results.append((df[0], res))
        results_with_error = []
        for result in results:
            if result[1].success:
                pass
            else:
                results_with_error.append(result)
        if len(results_with_error) == 0:
            logging.info('GreatExpectations run SUCCESSFUL, without missed expectations.')
        else:
            logging.error(f'FAILED: {len(results_with_error)} out of {len(ge_dfs)} tested datasets have failed.')
            raise AirflowException(results_with_error)


class GreatExpectationsBaseOperator(BaseOperator):

    def __init__(
        self,
        ckan_metadata_url: str,
        sftp_connection_id: str,
        store_path: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.ckan_metadata_url = ckan_metadata_url
        self.sftp_connection_id = sftp_connection_id
        self.store_path = store_path

        # Extract metadata from ckan
        r = requests.get(self.ckan_metadata_url)
        self._meta = json.loads(r.text)
        self.ckan_name = self._meta['result']['name']
        self.ckan_id = self._meta['result']['id']

        # Set general attributes
        self.remote_filepath = os.path.join(self.store_path, self.ckan_id)


class ExpectTableColumnsToMatchOrderedListOperator(GreatExpectationsBaseOperator, ExpectationMixin):
    """Apply Expectation"""

    METHOD_NAME = 'expect_table_columns_to_match_ordered_list'

    def __init__(
        self,
        ge_col_list: List[str],
        ckan_metadata_url: str,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        df_apply_func: Optional[List[Tuple[str, object]]] = None,
        **kwargs
    ):
        super().__init__(
            ckan_metadata_url=ckan_metadata_url,
            sftp_connection_id=sftp_connection_id,
            store_path=store_path,
            **kwargs
        )
        self.ge_parameters = {
            'column_list': ge_col_list,
        }
        self.df_apply_func = df_apply_func

    def execute(self, context):
        self.apply_expectations(self.METHOD_NAME, **self.ge_parameters)


class ExpectColumnDistinctValuesToBeInSet(GreatExpectationsBaseOperator, ExpectationMixin):
    """Apply Expectation"""

    METHOD_NAME = 'expect_column_distinct_values_to_be_in_set'

    def __init__(
        self,
        ge_col: str,
        ge_values: list,
        ckan_metadata_url: str,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        df_apply_func: Optional[List[Tuple[str, object]]] = None,
        **kwargs
    ):
        super().__init__(
            ckan_metadata_url=ckan_metadata_url,
            sftp_connection_id=sftp_connection_id,
            store_path=store_path,
            **kwargs
        )
        self.ge_parameters = {
            'column': ge_col,
            'value_set': ge_values,
        }
        self.df_apply_func = df_apply_func

    def execute(self, context):
        self.apply_expectations(self.METHOD_NAME, **self.ge_parameters)


class ExpectColumnValuesToBeDateutilParseable(GreatExpectationsBaseOperator, ExpectationMixin):
    """Apply Expectation"""

    METHOD_NAME = 'expect_column_values_to_be_dateutil_parseable'

    def __init__(
        self,
        ge_col: str,
        ckan_metadata_url: str,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        df_apply_func: Optional[List[Tuple[str, object]]] = None,
        **kwargs
    ):
        super().__init__(
            ckan_metadata_url=ckan_metadata_url,
            sftp_connection_id=sftp_connection_id,
            store_path=store_path,
            **kwargs
        )
        self.ge_parameters = {
            'column': ge_col,
        }
        self.df_apply_func = df_apply_func

    def execute(self, context):
        self.apply_expectations(self.METHOD_NAME, **self.ge_parameters)


class ExpectColumnValuesToBeBetween(GreatExpectationsBaseOperator, ExpectationMixin):
    """Apply Expectation"""

    METHOD_NAME = 'expect_column_values_to_be_between'

    def __init__(
        self,
        ckan_metadata_url: str,
        ge_col: str,
        ge_min: Optional[float] = None,
        ge_max: Optional[float] = None,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        df_apply_func: Optional[List[Tuple[str, object]]] = None,
        **kwargs
    ):
        super().__init__(
            ckan_metadata_url=ckan_metadata_url,
            sftp_connection_id=sftp_connection_id,
            store_path=store_path,
            **kwargs
        )
        self.ge_parameters = {
            'column': ge_col,
            'min_value': ge_min,
            'max_value': ge_max
        }
        self.df_apply_func = df_apply_func

    def execute(self, context):
        self.apply_expectations(self.METHOD_NAME, **self.ge_parameters)


class ExpectColumnMedianToBeBetween(GreatExpectationsBaseOperator, ExpectationMixin):
    """Apply Expectation"""

    METHOD_NAME = 'expect_column_median_to_be_between'

    def __init__(
        self,
        ckan_metadata_url: str,
        ge_col: str,
        ge_min: Optional[float] = None,
        ge_max: Optional[float] = None,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        df_apply_func: Optional[List[Tuple[str, object]]] = None,
        **kwargs
    ):
        super().__init__(
            ckan_metadata_url=ckan_metadata_url,
            sftp_connection_id=sftp_connection_id,
            store_path=store_path,
            **kwargs
        )
        self.ge_parameters = {
            'column': ge_col,
            'min_value': ge_min,
            'max_value': ge_max
        }
        self.df_apply_func = df_apply_func

    def execute(self, context):
        self.apply_expectations(self.METHOD_NAME, **self.ge_parameters)


class ExpectColumnMeanToBeBetween(GreatExpectationsBaseOperator, ExpectationMixin):
    """Apply Expectation"""

    METHOD_NAME = 'expect_column_median_to_be_between'

    def __init__(
        self,
        ckan_metadata_url: str,
        ge_col: str,
        ge_min: Optional[float] = None,
        ge_max: Optional[float] = None,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        df_apply_func: Optional[List[Tuple[str, object]]] = None,
        **kwargs
    ):
        super().__init__(
            ckan_metadata_url=ckan_metadata_url,
            sftp_connection_id=sftp_connection_id,
            store_path=store_path,
            **kwargs
        )
        self.ge_parameters = {
            'column': ge_col,
            'min_value': ge_min,
            'max_value': ge_max
        }
        self.df_apply_func = df_apply_func

    def execute(self, context):
        self.apply_expectations(self.METHOD_NAME, **self.ge_parameters)


class ExpectColumnValuesToMatchRegexList(GreatExpectationsBaseOperator, ExpectationMixin):
    """Apply Expectation"""

    METHOD_NAME = 'expect_column_values_to_match_regex_list'

    def __init__(
        self,
        ge_col: str,
        ge_regex_list: List[str],
        ckan_metadata_url: str,
        sftp_connection_id: str = _DEFAULT_SFTP_CONN_ID,
        store_path: str = os.path.join('data', 'ckan'),
        df_apply_func: Optional[List[Tuple[str, object]]] = None,
        **kwargs
    ):
        super().__init__(
            ckan_metadata_url=ckan_metadata_url,
            sftp_connection_id=sftp_connection_id,
            store_path=store_path,
            **kwargs
        )
        self.ge_parameters = {
            'column': ge_col,
            'regex_list': ge_regex_list,
        }
        self.df_apply_func = df_apply_func

    def execute(self, context):
        self.apply_expectations(self.METHOD_NAME, **self.ge_parameters)
