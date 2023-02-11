from airflow.models.baseoperator import BaseOperator


class CkanBaseOperator(BaseOperator):

    def __init__(self, ckan_name: str, **kwargs):
        super().__init__(**kwargs)
        self.ckan_name = ckan_name

    def get_dag_runtime(self, context):
        return context.get('dag_run').execution_date
