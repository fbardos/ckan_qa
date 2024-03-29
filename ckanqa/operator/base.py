import datetime as dt

from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator


class CkanBaseOperator(BaseOperator):

    def __init__(self, ckan_name: str, **kwargs):
        super().__init__(**kwargs)
        self.ckan_name = ckan_name

    def get_dag_runtime(self, airflow_context) -> dt.datetime:
        return airflow_context.get('dag_run').execution_date

    def get_dag_runtime_iso_8601_basic(self, airflow_context) -> str:
        format = Variable.get('CKANQA__STRFTIME_FORMAT')
        return self.get_dag_runtime(airflow_context).strftime(format)

    def get_s3_prefix(self, airflow_context) -> str:
        dag_runtime = self.get_dag_runtime_iso_8601_basic(airflow_context)
        return '/'.join([self.ckan_name, dag_runtime])


class CkanGeBaseOperator(CkanBaseOperator):

    def __init__(self, ckan_name: str, suite_name: str, **kwargs):
        super().__init__(ckan_name, **kwargs)
        self.suite_name = suite_name
