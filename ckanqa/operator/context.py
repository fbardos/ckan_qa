from ckanqa.context import CkanContext
from ckanqa.operator.ckan import CkanBaseOperator


class CkanContextSetter(CkanBaseOperator):

    def __init__(self, ckan_name: str, **kwargs):
        super().__init__(ckan_name=ckan_name, **kwargs)

    def execute(self, context):
        CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=False)
