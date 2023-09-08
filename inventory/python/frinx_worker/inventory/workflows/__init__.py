from frinx.common.workflow.service import ServiceWorkflowsImpl
from frinx.common.workflow.task import DoWhileTask
from frinx.common.workflow.task import DynamicForkTask
from frinx.common.workflow.task import DynamicForkTaskInputParameters
from frinx.common.workflow.task import JoinTask
from frinx.common.workflow.task import JsonJqTask
from frinx.common.workflow.task import JsonJqTaskInputParameters
from frinx.common.workflow.task import LambdaTask
from frinx.common.workflow.task import LambdaTaskInputParameters
from frinx.common.workflow.task import SimpleTask
from frinx.common.workflow.task import SimpleTaskInputParameters
from frinx.common.workflow.workflow import FrontendWFInputFieldType
from frinx.common.workflow.workflow import WorkflowImpl
from frinx.common.workflow.workflow import WorkflowInputField

from .. import InventoryService


class Inventory(ServiceWorkflowsImpl):

    class InstallAllFromInventory(WorkflowImpl):
        name: str = 'Install_all_from_inventory'
        version: int = 1
        description: str = 'Install all devices from device inventory or by label'
        labels: list[str] = ['INVENTORY']
        timeout_seconds = 3600

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            labels: WorkflowInputField = WorkflowInputField(
                name='labels',
                frontend_default_value=None,
                description='Select device labels',
                type=FrontendWFInputFieldType.STRING,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response_body: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:

            get_labels = SimpleTask(
                name=InventoryService.InventoryGetLabelsId,
                task_reference_name='get_labels',
                input_parameters=SimpleTaskInputParameters(
                    labels=workflow_inputs.labels.wf_input
                )
            )

            get_pages_cursors = SimpleTask(
                name=InventoryService.InventoryGetPagesCursors,
                task_reference_name='get_pages_cursors',
                input_parameters=SimpleTaskInputParameters(
                    labels=get_labels.output_ref('labels_id')
                )
            )

            convert_to_string = LambdaTask(
                name='convert_to_string',
                task_reference_name='convert_to_string',
                input_parameters=LambdaTaskInputParameters(
                    lambdaValue=get_pages_cursors.output_ref('cursors_groups'),
                    script_expression='return $.lambdaValue["loop_"+($.iterator-1).toString()]',
                    iterator='${loop_device_pages.output.iteration}',
                ),
            )

            get_pages_cursors_fork_task = SimpleTask(
                name=InventoryService.InventoryGetPagesCursorsForkTasks,
                task_reference_name='get_pages_cursors_fork_task',
                input_parameters=SimpleTaskInputParameters(
                    task=InventoryService.InventoryInstallInBatch().WorkerDefinition().name,
                    cursors_groups=convert_to_string.output_ref('result'),
                )
            )

            fork = DynamicForkTask(
                name='dyn_fork',
                task_reference_name='dyn_fork',
                dynamic_fork_tasks_param='dynamic_tasks',
                dynamic_fork_tasks_input_param_name='dynamic_tasks_input',
                input_parameters=DynamicForkTaskInputParameters(
                    dynamic_tasks=get_pages_cursors_fork_task.output_ref('dynamic_tasks'),
                    dynamic_tasks_input=get_pages_cursors_fork_task.output_ref('dynamic_tasks_input')
                )
            )

            join = JoinTask(
                name='join',
                task_reference_name='join'
            )

            loop_task = DoWhileTask(
                name='loop_device_pages',
                task_reference_name='loop_device_pages',
                input_parameters=dict(
                    value=get_pages_cursors.output_ref('number_of_groups')
                ),
                loop_condition="if ( $.loop_device_pages['iteration'] < $.value ) { true; } else { false; }",
                loop_over=[
                    convert_to_string,
                    get_pages_cursors_fork_task,
                    fork,
                    join
                ]
            )

            join_results = JsonJqTask(
                name='join_results',
                task_reference_name='join_results',
                input_parameters=JsonJqTaskInputParameters(
                    query_expression='{output: .data[]|iterables|.join}',
                    data=loop_task.output_ref()
                )
            )

            self.tasks = [
                get_labels,
                get_pages_cursors,
                loop_task,
                join_results
            ]

    class InstallInBatch(WorkflowImpl):
        name: str = 'INVENTORY_install_in_batch'
        version: int = 1
        description: str = 'Install devices in batch'
        labels: list[str] = ['INVENTORY']
        timeout_seconds = 3600

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            devices: WorkflowInputField = WorkflowInputField(
                name='devices',
                frontend_default_value=None,
                description='Devices list',
                type=FrontendWFInputFieldType.TEXTAREA,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response_body: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:

            self.tasks.append(SimpleTask(
                    name=InventoryService.InventoryInstallInBatch,
                    task_reference_name='install_in_batch',
                    input_parameters=SimpleTaskInputParameters(
                        devices=workflow_inputs.devices.wf_input
                    )
                )
            )

    class UninstallAllFromInventory(WorkflowImpl):
        name: str = 'Uninstall_all_from_inventory'
        version: int = 1
        description: str = 'Uninstall all devices from device inventory or by label'
        labels: list[str] = ['INVENTORY']
        timeout_seconds = 3600

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            labels: WorkflowInputField = WorkflowInputField(
                name='labels',
                frontend_default_value=None,
                description='Select device labels',
                type=FrontendWFInputFieldType.STRING,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response_body: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:

            get_labels = SimpleTask(
                name=InventoryService.InventoryGetLabelsId,
                task_reference_name='get_labels',
                input_parameters=SimpleTaskInputParameters(
                    labels=workflow_inputs.labels.wf_input
                )
            )

            get_pages_cursors = SimpleTask(
                name=InventoryService.InventoryGetPagesCursors,
                task_reference_name='get_pages_cursors',
                input_parameters=SimpleTaskInputParameters(
                    labels=get_labels.output_ref('labels_id')
                )
            )

            convert_to_string = LambdaTask(
                name='convert_to_string',
                task_reference_name='convert_to_string',
                input_parameters=LambdaTaskInputParameters(
                    lambdaValue=get_pages_cursors.output_ref('cursors_groups'),
                    script_expression='return $.lambdaValue["loop_"+($.iterator-1).toString()]',
                    iterator='${loop_device_pages.output.iteration}',
                ),
            )

            get_pages_cursors_fork_task = SimpleTask(
                name=InventoryService.InventoryGetPagesCursorsForkTasks,
                task_reference_name='get_pages_cursors_fork_task',
                input_parameters=SimpleTaskInputParameters(
                    task='INVENTORY_uninstall_in_batch',
                    cursors_groups=convert_to_string.output_ref('result'),
                )
            )

            fork = DynamicForkTask(
                name='dyn_fork',
                task_reference_name='dyn_fork',
                dynamic_fork_tasks_param='dynamic_tasks',
                dynamic_fork_tasks_input_param_name='dynamic_tasks_input',
                input_parameters=DynamicForkTaskInputParameters(
                    dynamic_tasks=get_pages_cursors_fork_task.output_ref('dynamic_tasks'),
                    dynamic_tasks_input=get_pages_cursors_fork_task.output_ref('dynamic_tasks_input')
                )
            )

            join = JoinTask(
                name='join',
                task_reference_name='join'
            )

            loop_task = DoWhileTask(
                name='loop_device_pages',
                task_reference_name='loop_device_pages',
                input_parameters=dict(
                    value=get_pages_cursors.output_ref('number_of_groups')
                ),
                loop_condition="if ( $.loop_device_pages['iteration'] < $.value ) { true; } else { false; }",
                loop_over=[
                    convert_to_string,
                    get_pages_cursors_fork_task,
                    fork,
                    join
                ]
            )

            join_results = JsonJqTask(
                name='join_results',
                task_reference_name='join_results',
                input_parameters=JsonJqTaskInputParameters(
                    query_expression='{output: .data[]|iterables|.join}',
                    data=loop_task.output_ref()
                )
            )

            self.tasks = [
                get_labels,
                get_pages_cursors,
                loop_task,
                join_results
            ]

            self.output_parameters = self.WorkflowOutput(
                response_body=join_results.output_ref('data')
            ).dict()

    class UninstallInBatch(WorkflowImpl):
        name: str = 'INVENTORY_uninstall_in_batch'
        version: int = 1
        description: str = 'Uninstall devices in batch'
        labels: list[str] = ['INVENTORY']
        timeout_seconds = 3600

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            devices: WorkflowInputField = WorkflowInputField(
                name='devices',
                frontend_default_value=None,
                description='Devices list',
                type=FrontendWFInputFieldType.TEXTAREA,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response_body: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:

            self.tasks.append(SimpleTask(
                    name=InventoryService.InventoryUninstallInBatch,
                    task_reference_name='uninstall_in_batch',
                    input_parameters=SimpleTaskInputParameters(
                        devices=workflow_inputs.devices.wf_input
                    )
                )
            )
