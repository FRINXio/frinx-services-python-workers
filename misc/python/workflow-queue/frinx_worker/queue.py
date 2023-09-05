import logging
import os
import threading
from datetime import datetime
from datetime import timedelta
from time import sleep
from typing import Optional
from typing import Any

import pydantic

from .models import Queue
from .models import QueueInfo
from .models import QueueRelease
from .models import QueueStatus
from .models import Resource
from .models import TaskResponse
from .collections import QueueCollection
from .collections import ResourceAllocationGraph

from . import get_mandatory
from . import get_optional
from . import parse_items
from . import finalize_response
from . import failed_response
from . import completed_response


from frinx.common.frinx_rest import CONDUCTOR_URL_BASE
from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.worker.worker import WorkerImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult

from frinx.common.type_aliases import DictAny

# from src.common.conductor.task.simple import ConductorTaskRegistry
# from src.common.conductor.task.simple import SimpleTaskImpl
# from src.common.conductor.task.simple import SimpleTaskInput
# from src.common.conductor.task.simple import SimpleTaskOutput
from src.common.logging import task_logger
from src.common.uniflow.controllers import WorkflowController


local_logs = logging.getLogger(__name__)

RESPONSE_TIMEOUT_SECONDS = 3600

queue_from_conductor: list[Queue] = []
queue_from_conductor_lock = threading.Lock()

queue_release_from_conductor: list[QueueRelease] = []
queue_release_from_conductor_lock = threading.Lock()

queue_to_conductor: list[TaskResponse] = []
queue_to_conductor_lock = threading.Lock()

active_queues: dict[str, Queue] = {}
active_queues_lock = threading.Lock()

waiting_queues: dict[str, Queue] = {}
waiting_queues_lock = threading.Lock()


def queue_collect(task: DictAny) -> None:
    try:
        task_id = get_mandatory(task, ['taskId'])
        source_wf_id = get_mandatory(task, ['inputData', 'sourceWorkflowId'])
        resources = get_mandatory(task, ['inputData', 'resources'])
        resources_context = get_optional(task, ['inputData', 'resources_context'])

        resources = parse_items(resources) if resources else []
        resources_context = parse_items(resources_context) if resources_context else []

        resources = ResourceManager.parse_resources(resources)
        resources_context = ResourceManager.parse_resources(resources_context)

        for resource in resources.values():
            if resource.limit < resource.used:
                raise ValueError(f'Requesting resources over the limit. [{resource}]')

        for resource in resources_context.values():
            if resource.limit < resource.used:
                raise ValueError(f'Requesting resources over the limit. [{resource}]')

        workflow_path = WorkflowController.get_workflow_path_with_retry(source_wf_id)

        queue = Queue(
            task_id=task_id,
            task=task,
            source_workflow_id=source_wf_id,
            queue_workflow_id=task['workflowInstanceId'],
            workflow_path=workflow_path,
            resources=resources,
            resources_context=resources_context,
            status=QueueStatus.PROCESSING,
            execution_start=datetime.now(),
        )

        with queue_from_conductor_lock:
            queue_from_conductor.append(queue)

    except Exception as err:
        task_logger.error('Failed to collect queue. err: %s', str(err))
        raise  # it will actually return immediately for this task - with the error (we can allow this task to fail - no dedicated thread)


def queue_release(task: DictAny) -> None:
    try:
        task_id = get_mandatory(task, ['taskId'])
        queue_workflow_id = get_mandatory(task, ['inputData', 'queue_workflow_id'])

        with queue_release_from_conductor_lock:
            global queue_release_from_conductor
            queue_release_from_conductor.append(
                QueueRelease(task_id=task_id, task=task, queue_workflow_id=queue_workflow_id)
            )

    except Exception as err:
        task_logger.error('Failed to release queue. err: %s', str(err))
        raise  # it will actually return immediately for this task - with the error (we can allow this task to fail - no dedicated thread)


class ResourceManager:
    def __init__(self) -> None:
        self.running_workflows: list[str] = []
        self.blocked_resources: dict[str, int] = {}
        self.waiting_queue: QueueCollection = QueueCollection()
        self.active_queue: QueueCollection = QueueCollection()
        self.rag = ResourceAllocationGraph()

    @staticmethod
    def parse_resources(resource_data: list[str] | list[DictAny]) -> dict[str, Resource]:
        def _as_strings(raw_data: list[str]) -> dict[str, Resource]:
            return {
                resource_name: Resource(name=resource_name, limit=1, used=1)
                for resource_name in raw_data
            }

        def _as_dicts(raw_data: list[DictAny]) -> dict[str, Resource]:
            resources = set()
            for resource in raw_data:
                try:
                    resources.add(
                        Resource(
                            name=resource['name'], limit=resource['limit'], used=resource['used']
                        )
                    )
                except KeyError as err:
                    local_logs.error('Resource field missing. err: %s', str(err))
                    raise err
            return {resource.name: resource for resource in resources}

        if all(
            [isinstance(resource, str) or isinstance(resource, int) for resource in resource_data]
        ):
            return _as_strings(resource_data)  # type: ignore[arg-type]
        elif all([isinstance(resource, dict) for resource in resource_data]):
            return _as_dicts(resource_data)  # type: ignore[arg-type]
        else:
            return {}

    def find_subworkflow_dependencies(self, queue: Queue) -> None:
        """Filters out only workflows with queues from the workflow path list."""
        queue.dependencies = []
        for workflow_id in queue.workflow_path:
            if workflow_id == queue.source_workflow_id:
                continue
            if workflow_id in self.rag.graph:
                queue.dependencies.append(workflow_id)

    def load_queues_for_processing(self) -> None:
        with queue_from_conductor_lock:
            global queue_from_conductor
            for queue in queue_from_conductor:
                if queue.task_id in self.waiting_queue:
                    self.waiting_queue.update_item(queue.task_id, execution_start=datetime.now())
                    continue

                if queue.task_id in self.active_queue:
                    continue

                self.waiting_queue.insert(queue)
                # Add workflow dependencies to RAG
                self.find_subworkflow_dependencies(queue)
                if queue.dependencies:
                    self.rag.add_workflow(queue.source_workflow_id)
                    for parent_workflow_id in queue.dependencies:
                        self.rag.add_workflow_dependency_edge(
                            parent_workflow_id, queue.source_workflow_id
                        )
                for resource in queue.resource_diff.values():
                    self.rag.add_resource_request_edge(queue.source_workflow_id, resource.name)

            queue_from_conductor = []

    def refresh_running_workflows(self) -> None:
        self.running_workflows = WorkflowController.get_running_workflows_with_retry()

    def unblock_queue_resources(self, queue: Queue) -> None:
        for resource in queue.resource_diff.values():
            if resource.name not in self.blocked_resources:
                local_logs.error('Resource %s not blocked.', resource.name)
                continue
            blocked_count = self.blocked_resources[resource.name]
            if blocked_count < resource.used:
                local_logs.error(
                    'Attempting to unblock %d of resource %s but only %d is blocked.',
                    resource.used,
                    resource.name,
                    blocked_count,
                )
                continue

            self.rag.remove_workflow(queue.source_workflow_id)
            self.blocked_resources[resource.name] -= resource.used
            if self.blocked_resources[resource.name] == 0:
                del self.blocked_resources[resource.name]

    def release_queue(self, queue_release_task: QueueRelease) -> None:
        try:
            queue = self.active_queue.pop_by_wf_id(queue_release_task.queue_workflow_id)
        except (KeyError, UnboundLocalError) as err:
            local_logs.error(
                'Could not get queue data of %s. err: %s',
                queue_release_task.queue_workflow_id,
                str(err),
            )
            with queue_to_conductor_lock:
                queue_to_conductor.append(
                    queue_release_task.emit_response(
                        status=QueueStatus.FAILED, response_body='Queue not found.'
                    )
                )
            return

        self.unblock_queue_resources(queue)

        with queue_to_conductor_lock:
            queue_to_conductor.append(
                queue_release_task.emit_response(
                    status=QueueStatus.COMPLETED,
                    response_body={'resources_context': queue.resources_context},
                )
            )

    def unblock_all_resources(self) -> None:
        with queue_release_from_conductor_lock:
            global queue_release_from_conductor
            release_tasks = queue_release_from_conductor.copy()
            queue_release_from_conductor = []

        for queue_release_task in release_tasks:
            self.release_queue(queue_release_task)

        queue: Queue
        for queue in self.active_queue[:]:  # type: ignore[assignment]
            if (
                queue.status == QueueStatus.COMPLETED
                and queue.source_workflow_id not in self.running_workflows
            ):
                self.unblock_queue_resources(queue)
                self.active_queue.remove(queue.task_id)

    def update_queue(self, queue: Queue, keep_waiting: bool = False) -> None:
        if not keep_waiting:
            try:
                self.waiting_queue.remove(queue.task_id)
            except KeyError as err:
                local_logs.warning(
                    'Queue %s not found in waiting_queue. err: %s', queue.task_id, str(err)
                )

        with queue_to_conductor_lock:
            global queue_to_conductor
            queue_to_conductor.append(queue.emit_response())

    def activate_queue(self, queue: Queue) -> None:
        try:
            self.active_queue.insert(queue)
        except ValueError as err:
            local_logs.error('Failed to activate queue %s. err: %s', queue.task_id, str(err))
            return

        queue.status = QueueStatus.COMPLETED
        for resource in queue.resource_diff.values():
            self.rag.allocate_resource(
                workflow_id=queue.source_workflow_id, resource_name=resource.name
            )
            if resource.name not in self.blocked_resources:
                self.blocked_resources[resource.name] = 0
            self.blocked_resources[resource.name] += resource.used

        self.update_queue(queue)

    def fail_queue(self, queue: Queue, error_message: Optional[str] = None) -> None:
        queue.status = QueueStatus.FAILED
        if error_message:
            queue.response_body = error_message
        self.update_queue(queue)

    def ping_queue(self, queue: Queue) -> None:
        queue.execution_start = datetime.now()
        self.update_queue(queue, keep_waiting=True)

    def is_queue_blocked(self, queue: Queue, reserved: set[str]) -> set[str]:
        blocking_resources = set()
        for resource in queue.resource_diff.values():
            if resource.name in reserved:
                blocking_resources.add(resource.name)
                continue

            if resource.used + self.blocked_resources.get(resource.name, 0) > resource.limit:
                blocking_resources.add(resource.name)
        return blocking_resources

    def process_waiting(self) -> None:
        reserved_resources: set[str] = set()
        queue: Queue
        for queue in self.waiting_queue[:]:  # type: ignore[assignment]
            # Do not accept queues without resources
            if not queue.resources:
                self.fail_queue(queue, error_message='No resources specified.')
                continue

            # All resource requirements are already satisfied by the resources_context
            if not queue.resource_diff:
                self.activate_queue(queue)
                continue

            # Do not let queue sit waiting for too long without updates
            if queue.execution_start and (datetime.now() - queue.execution_start) * 2 > timedelta(
                seconds=RESPONSE_TIMEOUT_SECONDS
            ):
                self.ping_queue(queue)
                continue

            # Keep the blocked queues in the waiting line
            if blocking_resources := self.is_queue_blocked(queue, reserved=reserved_resources):
                # Check for a deadlock
                if queue.dependencies and (cycle := self.rag.find_cycle(queue.source_workflow_id)):
                    self.resolve_deadlock(queue, cycle)
                    continue

                reserved_resources |= blocking_resources
                continue

            # Now it is safe to lock resources
            self.activate_queue(queue)

    def resolve_deadlock(self, queue: Queue, cycle: list[str]) -> None:
        cycle_description = ' -> '.join(cycle)
        self.rag.cancel_requests_of_workflow(queue.source_workflow_id)
        self.fail_queue(queue, error_message=f'Deadlock detected: {cycle_description}')


def queue_process(cc: ConductorTaskRegistry) -> None:
    resource_manager = ResourceManager()

    while True:
        try:
            resource_manager.load_queues_for_processing()

            sleep(0.5)

            resource_manager.refresh_running_workflows()

            resource_manager.unblock_all_resources()

            if len(resource_manager.waiting_queue) > 0:
                resource_manager.process_waiting()

            # Publish active queues to global buffer
            with active_queues_lock:
                global active_queues
                active_queues = {
                    queue.source_workflow_id: queue for queue in resource_manager.active_queue
                }

            # Publish waiting queues to global buffer
            with waiting_queues_lock:
                global waiting_queues
                waiting_queues = {
                    queue.source_workflow_id: queue for queue in resource_manager.waiting_queue
                }

        except Exception as err:
            local_logs.error('Failed to process queue. err: %s', str(err))


def update_conductor(cc: ConductorTaskRegistry) -> None:
    to_send_list: list[TaskResponse] = []

    while True:
        try:
            with queue_to_conductor_lock:
                global queue_to_conductor
                to_send_list += queue_to_conductor
                queue_to_conductor = []

            sleep(0.5)

            if not to_send_list:
                continue

            for task_response in to_send_list[:]:
                try:
                    match task_response.status:
                        case QueueStatus.COMPLETED:
                            resp = completed_response(task_response.response_body)
                            log_msg = 'Task %s completed.'
                        case QueueStatus.FAILED:
                            resp = failed_response(task_response.response_body)
                            log_msg = 'Task %s failed.'
                        case QueueStatus.PROCESSING:
                            resp = finalize_response(
                                QueueStatus.PROCESSING, task_response.response_body
                            )
                            log_msg = 'Task %s is in progress.'
                        case _:
                            resp = {}
                            log_msg = ''

                    cc.update_task(task_response.task, resp, [])  # type: ignore[attr-defined]
                    to_send_list.remove(task_response)
                    local_logs.info(log_msg, task_response.task['taskId'])
                except Exception as err:
                    local_logs.error(
                        'Failed to update conductor task %s. err: %s',
                        task_response.task['taskId'],
                        str(err),
                    )
        except Exception as err:
            local_logs.error('Failed to update conductor task. err: %s', str(err))



    cc.register(
        'Queue',
        {
            'description': '{"description": "Task implementing workflow queue",'
            ' "labels": ["Queue"]}',
            'inputKeys': ['sourceWorkflowId', 'resources'],
            'outputKeys': [],
            'timeoutSeconds': 0,
            'responseTimeoutSeconds': RESPONSE_TIMEOUT_SECONDS,
        },
        queue_collect,
        limit_to_thread_count=10,
        non_returning=True,
    )

class Queue(WorkerImpl):

    class ExecutionProperties(TaskExecutionProperties):
        exclude_empty_inputs: bool = True
        transform_string_to_json_valid: bool = False

    class WorkerDefinition(TaskDefinition):
        name = 'QUEUE'
        description = 'Task implementing workflow queue'
        labels = ['QUEUE']
        max_thread_count = 1
        timeout_seconds = 0
        response_timeout_seconds = 3600
        limit_to_thread_count = 10

    class WorkerInput(TaskInput):
        source_workflow_id: list[str] = pydantic.Field(alias='sourceWorkflowId')
        resources: list[str]

    class WorkerOutput(TaskOutput):
        queue_info: list[QueueInfo]

    def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:


class GetQueueDetails(WorkerImpl):

    class ExecutionProperties(TaskExecutionProperties):
        exclude_empty_inputs: bool = True
        transform_string_to_json_valid: bool = False

    class WorkerDefinition(TaskDefinition):
        name = 'QUEUE_get_queue_details'
        description = 'Shows id, name and locked resources of the workflow(s) in the same queue'
        labels = ['QUEUE']
        max_thread_count = 1

    class WorkerInput(TaskInput):
        workflow_ids: list[str]

    class WorkerOutput(TaskOutput):
        queue_info: list[QueueInfo]

    def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
        """Returns id, name and locked resources of the workflow(s) in the same queue"""

        with waiting_queues_lock:
            global waiting_queues
            currently_waiting_queues: dict[str, Queue] = waiting_queues.copy()

        with active_queues_lock:
            global active_queues
            currently_active_queues: dict[str, Queue] = active_queues.copy()

        running_wfs_with_queue = [
            wf_id for wf_id in worker_input.workflow_ids if wf_id in currently_waiting_queues
        ]

        if not running_wfs_with_queue:
            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(queue_info=[])
            )

        queues_with_same_resources = set()

        for input_wf_id in running_wfs_with_queue:
            input_wf_queue = currently_waiting_queues[input_wf_id]
            input_wf_resources = input_wf_queue.resource_diff
            input_wf_resources_names = set(input_wf_resources.keys())

            for wf_id, queue in currently_active_queues.items():
                resources_intersection = input_wf_resources_names.intersection(
                    set(queue.resource_union.keys())
                )
                if resources_intersection:
                    queues_with_same_resources.add(queue)

        sorted_queues_with_same_resources = list(
            sorted(list(queues_with_same_resources), key=lambda q: q.execution_start, reverse=False)  # type: ignore
        )

        queue_info = []

        for queue in sorted_queues_with_same_resources:
            if queue.source_workflow_id in worker_input.workflow_ids:
                continue

            parent_workflow = WorkflowController.search_workflow_metadata_with_retry(
                queue.source_workflow_id
            )
            queue_data = QueueInfo(
                parent_wf_id=queue.source_workflow_id,
                parent_wf_name=parent_workflow['workflowName'],
                resources_locked=list(queue.resource_diff.keys()),
            )

            queue_info.append(queue_data)

        return TaskResult(
            status=TaskResultStatus.COMPLETED,
            output=self.WorkerOutput(queue_info=queue_info)
        )


class SetQueueResourcesLimit(WorkerImpl):

    class ExecutionProperties(TaskExecutionProperties):
        exclude_empty_inputs: bool = True
        transform_string_to_json_valid: bool = False

    class WorkerDefinition(TaskDefinition):
        name = 'QUEUE_set_queue_resources_limit'
        description = 'Sets limit on all resources for the workflow queue'
        labels = ['QUEUE']
        max_thread_count = 1

    class WorkerInput(TaskInput):
        resources: list[str]
        limit: list[str] = pydantic.Field(
            metadata={
                'description': (
                    """
                    limit for resources, can  be an integer value or a reference to a limit defined in an 
                    environment variable (if the env variable is not set, defaults to 1)"
                    """
                )
            }
        )

        @pydantic.validator('limit')
        def validate_limit_fields(cls, v: int | str) -> int:
            if isinstance(v, str):
                try:
                    limit = int(os.getenv(v, 1))
                except ValueError as err:
                    raise ValueError(f'Referenced resource limit {v} invalid ({str(err)})')
            else:
                limit = v

            if limit < 1:
                raise ValueError(f'Minimal limit for resources in queue is 1, {limit} given.')
            return limit

    class WorkerOutput(TaskOutput):
        resources: list[DictAny]

    def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
        """Returns resources with new limits"""

        return TaskResult(
            status=TaskResultStatus.COMPLETED,
            output=self.WorkerOutput(resources=[
                {'name': resource, 'limit': worker_input.limit, 'used': 1}
                for resource in worker_input.resources
            ])
        )


def start(cc: ConductorTaskRegistry) -> None:
    local_logs.info('Starting queue workers [v2]')

    cc.register(
        'Queue',
        {
            'description': '{"description": "Task implementing workflow queue",'
            ' "labels": ["Queue"]}',
            'inputKeys': ['sourceWorkflowId', 'resources'],
            'outputKeys': [],
            'timeoutSeconds': 0,
            'responseTimeoutSeconds': RESPONSE_TIMEOUT_SECONDS,
        },
        queue_collect,
        limit_to_thread_count=10,
        non_returning=True,
    )
    cc.register(
        'Queue_release',
        {
            'description': '{"description": "Task implementing release of workflow queue",'
            ' "labels": ["Queue"]}',
            'inputKeys': ['queue_workflow_id'],
            'outputKeys': [],
            'timeoutSeconds': 0,
            'responseTimeoutSeconds': RESPONSE_TIMEOUT_SECONDS,
        },
        queue_release,
        limit_to_thread_count=10,
        non_returning=True,
    )

    # Background thread for processing queues
    cc.register_background_thread(queue_process)

    # Background thread for updating conductor
    cc.register_background_thread(update_conductor)
