from ckanqa.context import CkanContext
from ckanqa.operator.ckan import CkanBaseOperator


class CkanContextSetter(CkanBaseOperator):

    def __init__(self, ckan_name: str, **kwargs):
        super().__init__(ckan_name=ckan_name, **kwargs)

    def execute(self, context):
        ckan_context = CkanContext.generate_context_from_airflow_execute(self, context, import_from_redis=False)
        ckan_context.datasource_name = ckan_context.default_datasource_name
        ckan_context.selected_checkpoint = ckan_context.default_checkpoint_name
        ckan_context.attach_empty_checkpoint_config()
