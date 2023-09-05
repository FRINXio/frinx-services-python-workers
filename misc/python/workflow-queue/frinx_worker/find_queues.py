from typing import Any

import requests
from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.frinx_rest import CONDUCTOR_URL_BASE
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl

from .models import QueueInfo
from .models import QueueInfoWithRoot

# from src.common.worker_helpers.make_tree import get_workflow_loader


class GetWorkflowsWithQueue(WorkerImpl):

    class ExecutionProperties(TaskExecutionProperties):
        exclude_empty_inputs: bool = True
        transform_string_to_json_valid: bool = False

    class WorkerDefinition(TaskDefinition):
        name = 'QUEUE_get_workflows_with_queue'
        description = 'Returns ids of workflows with queue'
        labels = ['QUEUE']
        max_thread_count = 1

    class WorkerInput(TaskInput):
        workflow_id: str

    class WorkerOutput(TaskOutput):
        workflows_with_queue: list[str]

    def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
        wf_loader = get_workflow_loader()
        wf_loader.init_tree_from(worker_input.workflow_id)

        workflows_with_queue = []

        for workflow in wf_loader:
            for task in workflow.workflow_data['tasks']:
                if task['taskType'] == 'SUB_WORKFLOW' and task['taskDefName'] == 'Workflow_queue':
                    workflows_with_queue.append(workflow.workflow_data['workflowId'])

        return TaskResult(
            status=TaskResultStatus.COMPLETED,
            output=self.WorkerOutput(workflows_with_queue=workflows_with_queue)
        )


class GetQueueInfoRoot(WorkerImpl):

    class ExecutionProperties(TaskExecutionProperties):
        exclude_empty_inputs: bool = True
        transform_string_to_json_valid: bool = False

    class WorkerDefinition(TaskDefinition):
        name = 'QUEUE_get_queue_details_root'
        description = 'Returns ids of workflows with queue'
        labels = ['QUEUE']
        max_thread_count = 1

    class WorkerInput(TaskInput):
        queue_info: list[QueueInfo]

    class WorkerOutput(TaskOutput):
        queue_info_with_root: list[QueueInfoWithRoot]

    @staticmethod
    def _get_wf_name_by_id(wf_id: str) -> str:
        url_wf_info = f'{CONDUCTOR_URL_BASE}/id/{wf_id}'
        wf_name: str = requests.get(url_wf_info).json()['result']['workflowName']
        return wf_name

    @classmethod
    def _get_service_order_id(cls, wf_id: str) -> int | None:
        url_wf_info = f'{CONDUCTOR_URL_BASE}/id/{wf_id}'
        wf_name = cls._get_wf_name_by_id(wf_id)

        if wf_name in {'TMF_ProcessOrder', 'TMF_ProcessOrderFailure'}:
            service_order_id: int = requests.get(url_wf_info).json()['result']['input'][
                'service_order_id'
            ]
            return service_order_id
        return None

    def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
        wf_loader = get_workflow_loader()

        queue_info_with_root = []

        for qi in worker_input.queue_info:
            root_wf_id = wf_loader.find_root(qi.parent_wf_id)
            queue_info_with_root_data = QueueInfoWithRoot(
                root_wf_id=root_wf_id,
                root_wf_name=self._get_wf_name_by_id(root_wf_id),
                service_order_id=self._get_service_order_id(root_wf_id),
                resources_locked=qi.resources_locked,
            )
            queue_info_with_root.append(queue_info_with_root_data)

        return TaskResult(
            status=TaskResultStatus.COMPLETED,
            output=self.WorkerOutput(queue_info_with_root=queue_info_with_root)
        )
