""" Factories for creating Airflow tasks.

"""
from typing import List, Tuple

from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration

from airflow.models.baseoperator import BaseOperator
from airflow.operators.dummy import DummyOperator
from ckanqa.config import ValidationConfig
from ckanqa.operator.greatexpectations import (GeBuildExpectationOperator,
                                               GeRemoveExpectationsOperator,
                                               GeRunValidator)


class ValidationTaskFactory:
    """Factory who creates subdags with tasks for each GE Validation.

    This is a counterpart for the general ValidationContext. The parameters get set here
    and then injected into each operator.

    Will create a subdag for every validation_name.

    Args:
        expectation_mappings: Map expectation configurations validation_names.
            Will create expectation suites automatically from validation_names.
            If suite_names are set, will not create multiple expectations suites with the same expectation
            config.
        **datasource_name (str): Name for GE Datasource
        **s3_prefix (str): Manually overwritten s3_prefix
        **suite_names (List[str]): Suite names, for each validation_name.
            If suite_names are set, it is possible to assign the same expectation suite to multiple
            validation_names.
            Must have the same length as validation_names.


    """

    def __init__(
        self,
        ckan_name: str,
        validation_configs: List[ValidationConfig],
        expectation_mappings: List[Tuple[ExpectationConfiguration, Tuple[str, ...]]],
        **kwargs
    ):
        self.ckan_name = ckan_name
        self.validation_configs = validation_configs
        self.expectation_mappings = expectation_mappings
        self.task_container = {}

        if arg := kwargs.get('datasource_name', None):
            self.datasource_name = arg
        if arg := kwargs.get('s3_prefix', None):
            self.s3_prefix = arg
        if arg := kwargs.get('suite_names', None):
            self.suite_names = arg

    def _add_task_containers(self) -> None:
        for config in self.validation_configs:
            self.task_container[config.validation_name] = []

    def _add_operators_removing_all_expectations(self) -> None:
        for config in self.validation_configs:
            task_id = f'removeexp_{config.validation_name}'
            task = GeRemoveExpectationsOperator(
                task_id=task_id,
                ckan_name=config.ckan_name,
                suite_name=config.suite_name,
            )
            self.task_container[config.validation_name].append(task)

    def _add_operators_expectation_creation(self) -> None:
        """If mapping is empty -> assign to all validations"""
        for config in self.validation_configs:
            expectation_configs = []

            # Iterate over expectation mappings and assign task, when mapping set for current validation
            for idx, (expectation_config, validation_mapping) in enumerate(self.expectation_mappings):

                # If empty iterator is passed as validation_mapping, apply to all validations
                if (config.validation_name in validation_mapping) or (len(validation_mapping) == 0):
                    expectation_configs.append(expectation_config)

            assert len(expectation_configs) > 0
            task_id = f'addexp_{config.validation_name}'
            task = GeBuildExpectationOperator(
                task_id=task_id,
                ckan_name=config.ckan_name,
                suite_name=config.suite_name,
                expectation_config=expectation_configs,
            )
            self.task_container[config.validation_name].append(task)

    def _add_operators_checkpoint_exec(self) -> None:
        for config in self.validation_configs:
            task_id = f'exec_{config.validation_name}'
            task = GeRunValidator(
                task_id=task_id,
                validation_config=config
            )
            self.task_container[config.validation_name].append(task)

    def _add_dummy_operator(self, name: str) -> None:
        for config in self.validation_configs:
            task_id = f'{name}_{config.validation_name}'
            task = DummyOperator(task_id=task_id)
            self.task_container[config.validation_name].append(task)


    def generate_tasks(self):
        """ Generates Airflow tasks for GX handling.

        Task order:
            1. Delete old expectations from suite (1 task)
            2. Generate and store new expectations (n tasks)
            3. Build Validator, BatchRequest and execute (1 task)
            4. Create empty Tasks at the start and beginning of task, then combine all tasks together.
        """

        # Add empty task container for every validation name
        self._add_task_containers()

        # Add DummyOperator at the beginning of Subdag
        self._add_dummy_operator('start')

        # Add Operator to remove old expectations
        self._add_operators_removing_all_expectations()

        # Add expectations for each validation name according to mapping
        self._add_operators_expectation_creation()

        # Build checkpoint and execute
        self._add_operators_checkpoint_exec()

        # Append DummyOperator at the beginning and ending of subdag
        self._add_dummy_operator('end')

    def connect_tasks(self, upstream_task: BaseOperator, downstream_task: BaseOperator) -> List[List[BaseOperator]]:
        """Connects the tasks from the subdags with the main DAG."""
        assert len(self.task_container.keys()) == len(self.validation_configs)

        # Build all tasks of subdags in nested list
        all_tasks_container = []
        for tasks in self.task_container.values():
            all_tasks_container.append(tasks)

        # Connect dags
        for subdag in all_tasks_container:

            # Set entry point
            upstream_task.set_downstream(subdag[0])

            # Set exit point
            downstream_task.set_upstream(subdag[-1])

            # Generate sequence for each subdag
            for idx, task in enumerate(subdag):
                if idx > 0:
                    if isinstance(task, list):
                        subdag[idx-1].set_downstream(task)
                    else:
                        task.set_upstream(subdag[idx-1])

        return all_tasks_container
