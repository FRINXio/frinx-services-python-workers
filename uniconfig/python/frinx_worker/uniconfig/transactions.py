import copy
import json
from typing import Optional

import requests

from frinx.common.type_aliases import DictAny, ListStr, ListAny
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx_worker.uniconfig import handle_response, TransactionContext


class Transactions(ServiceWorkersImpl):

    class TransactionCloseJoin(WorkerImpl):
        from frinx.common.conductor_enums import TaskResultStatus

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: str = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_transactions_close_join'
            description: str = 'Format closed and unclosed transactions'
            labels: list[str] = ["BASICS", "TX"]

        class WorkerInput(TaskInput):
            input: DictAny

        class WorkerOutput(TaskOutput):
            closed: DictAny
            unclosed: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            closed_all = {}
            unclosed_all = {}

            for k, v in worker_input.input.items():

                closed_all[k] = v.get('closed')
                unclosed_all[k] = v.get('unclosed')

            return TaskResult(
                status=self.TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    closed=closed_all,
                    unclosed=unclosed_all
                )
            )

    class ZoneListToDynFork(WorkerImpl):
        from frinx.common.conductor_enums import TaskResultStatus
        from frinx.common.frinx_rest import KRAKEND_URL_BASE
        from frinx.common.frinx_rest import UNICONFIG_ZONE_URL_TEMPLATE

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: str = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_zone_list_to_dyn_fork'
            description: str = 'Convert zone list to dynamic fork input'
            labels: list[str] = ["BASICS", "UNICONFIG", "TX"]

        class WorkerInput(TaskInput):
            zones: Optional[ListStr]
            context: Optional[DictAny]

        class WorkerOutput(TaskOutput):
            dynamic_tasks: ListAny
            dynamic_tasks_i: DictAny

        @classmethod
        def _get_zones_from_krakend(cls) -> list[str]:
            zones = []
            response = requests.get(url=f'{cls.KRAKEND_URL_BASE}/static/list/uniconfig')
            response = response.json()
            instances = response.get('instances', [])
            for instance in instances:
                zones.append(cls.UNICONFIG_ZONE_URL_TEMPLATE.format(uc=instance))
            return zones

        @classmethod
        def _exclude_bad_zones_from_context(cls, context: dict) -> list[str]:
            zones: list[str] = list(context.keys())
            for zone, cookies in context.items():
                try:
                    TransactionContext(**cookies)
                except ValueError:
                    zones.remove(zone)
            return zones

        @classmethod
        def _prepare_dyn_fork(cls, zones: list[str]) -> tuple[ListAny, DictAny]:
            # Execute UC_TX_start subworkflow for zones, where transaction is not created
            dynamic_tasks = []
            dynamic_tasks_i = {}
            for task_ref in range(0, len(zones)):
                dynamic_tasks.append(
                    {
                        'name': 'UNICONFIG_Create_transaction_RPC',
                        'taskReferenceName': str(task_ref),
                    }
                )
                dynamic_tasks_i[str(task_ref)] = dict(
                    uniconfig_url_base=zones[task_ref]
                )
            return dynamic_tasks, dynamic_tasks_i

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            # Get all deployed uniconfig zones from krakend when not explicitly selected
            zones = worker_input.zones
            if zones is None:
                zones = self._get_zones_from_krakend()

            # exclude existing zones with wrong cookies from context
            context_zones = []
            if worker_input.context is not None:
                context_zones = self._exclude_bad_zones_from_context(worker_input.context['zones'])

            extracted_zones = [zone for zone in zones if zone not in context_zones]
            dynamic_tasks, dynamic_tasks_i = self._prepare_dyn_fork(extracted_zones)

            return TaskResult(
                status=self.TaskResultStatus.COMPLETED,
                logs=['Dynamic fork generator worker invoked successfully'],
                output=self.WorkerOutput(
                    dynamic_tasks_i=dynamic_tasks_i,
                    dynamic_tasks=dynamic_tasks
                )
            )

    class ZoneListJoinContext(WorkerImpl):
        from frinx.common.conductor_enums import TaskResultStatus

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: str = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_zone_list_join_context'
            description: str = 'Convert zone list to dynamic fork input'
            labels: list[str] = ["BASICS", "UNICONFIG", "TX"]

        class WorkerInput(TaskInput):
            data: Optional[DictAny]
            context: Optional[DictAny]

        class WorkerOutput(TaskOutput):
            context: DictAny

        @classmethod
        def _remove_bad_context_zones(cls, context: dict) -> dict:
            zones = copy.deepcopy(context['zones'])
            for zone, cookies in zones.items():
                try:
                    TransactionContext(**cookies)
                except ValueError:
                    context['zones'].pop(zone)
            return context

        @classmethod
        def from_join_to_zones_format(cls, data):
            zones = {}
            for key, value in data.items():
                if "uniconfig_url_base" in value:
                    url_base = value["uniconfig_url_base"]
                    del value["uniconfig_url_base"]
                    zones[url_base] = value
            return zones

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            context = copy.deepcopy(worker_input.context)
            data = worker_input.data

            if context is None:
                context = dict(zones=dict())

            context = self._remove_bad_context_zones(context)

            if data is not None:
                data = self.from_join_to_zones_format(data)

                for zone, transaction in data.items():
                    context['zones'][zone] = transaction

            return TaskResult(
                status=self.TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    context=context,
                )
            )

    class ContextToDynFork(WorkerImpl):
        from frinx.common.conductor_enums import TaskResultStatus

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: str = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_context_to_dyn_fork'
            description: str = 'Convert context to dynamic fork input'
            labels: list[str] = ["BASICS", "UNICONFIG", "TX"]

        class WorkerInput(TaskInput):
            context: Optional[DictAny]

        class WorkerOutput(TaskOutput):
            dynamic_tasks: ListAny
            dynamic_tasks_i: DictAny

        @classmethod
        def _remove_bad_context_zones(cls, context: dict) -> dict:
            zones = copy.deepcopy(context['zones'])
            for zone, cookies in zones.items():
                try:
                    TransactionContext(**cookies)
                except ValueError:
                    context['zones'].pop(zone)
            return context

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            dynamic_tasks = []
            dynamic_tasks_i = {}

            if worker_input.context is not None:

                for k, v in worker_input.context['zones'].items():
                    v = TransactionContext(**v)

                    dynamic_tasks.append(
                        {
                            'name': 'UNICONFIG_Close_transaction_RPC',
                            'taskReferenceName': k,
                        }
                    )
                    dynamic_tasks_i[k] = dict(
                        transaction_id=v.transaction_id,
                        uniconfig_server_id=v.uniconfig_server_id,
                        uniconfig_url_base=k
                    )

            return TaskResult(
                status=self.TaskResultStatus.COMPLETED,
                logs=['Dynamic fork generator worker invoked successfully'],
                output=self.WorkerOutput(
                    dynamic_tasks_i=dynamic_tasks_i,
                    dynamic_tasks=dynamic_tasks
                )
            )

    class ClosedListJoinContext(WorkerImpl):
        from frinx.common.conductor_enums import TaskResultStatus

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: str = True
            parse_string_to_list: list[str] = ['zones']

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_zone_list_join_context'
            description: str = 'Convert zone list to dynamic fork input'
            labels: list[str] = ["BASICS", "UNICONFIG", "TX"]

        class WorkerInput(TaskInput):
            data: DictAny = {}
            context: DictAny = {}

        class WorkerOutput(TaskOutput):
            context: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            for key, value in worker_input.data.items():
                if "uniconfig_url_base" in value:
                    url_base = value["uniconfig_url_base"]
                    del value["uniconfig_url_base"]
                    worker_input.context[url_base] = value

            return TaskResult(
                status=self.TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    context=worker_input.context,
                )
            )

    class ContextToDynForkCommit(WorkerImpl):
        from frinx.common.conductor_enums import TaskResultStatus

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: str = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_context_to_dyn_fork_commit'
            description: str = 'Convert context to dynamic fork input'
            labels: list[str] = ["BASICS", "UNICONFIG", "TX"]

        class WorkerInput(TaskInput):
            context: Optional[DictAny]

        class WorkerOutput(TaskOutput):
            dynamic_tasks: ListAny
            dynamic_tasks_i: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            dynamic_tasks = []
            dynamic_tasks_i = {}

            if worker_input.context is not None:
                for k, v in worker_input.context.items():
                    v = TransactionContext(**v)

                    dynamic_tasks.append(
                        {
                            'name': 'UNICONFIG_Commit_transaction_RPC',
                            'taskReferenceName': k,
                        }
                    )
                    dynamic_tasks_i[k] = dict(
                        transaction_id=v.transaction_id,
                        uniconfig_server_id=v.uniconfig_server_id,
                        uniconfig_url_base=k
                    )

            return TaskResult(
                status=self.TaskResultStatus.COMPLETED,
                logs=['Dynamic fork generator worker invoked successfully'],
                output=self.WorkerOutput(
                    dynamic_tasks_i=dynamic_tasks_i,
                    dynamic_tasks=dynamic_tasks
                )
            )

    class ContextToLoop(WorkerImpl):
        from frinx.common.conductor_enums import TaskResultStatus
        from frinx.common.frinx_rest import KRAKEND_URL_BASE
        from frinx.common.frinx_rest import UNICONFIG_ZONE_URL_TEMPLATE

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: str = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_context_to_loop'
            description: str = 'Convert context to dynamic fork input'
            labels: list[str] = ["BASICS", "UNICONFIG", "TX"]

        class WorkerInput(TaskInput):
            zones: Optional[ListStr]
            context: Optional[DictAny]

        class WorkerOutput(TaskOutput):
            count: int
            zones: list[str]

        @classmethod
        def _get_zones_from_krakend(cls) -> list[str]:
            zones = []
            response = requests.get(url=f'{cls.KRAKEND_URL_BASE}/static/list/uniconfig')
            response = response.json()
            instances = response.get('instances', [])
            for instance in instances:
                zones.append(cls.UNICONFIG_ZONE_URL_TEMPLATE.format(uc=instance))
            return zones

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            # Get all deployed uniconfig zones when not explicitly selected
            zones = worker_input.zones
            if zones is None:
                zones = self._get_zones_from_krakend()

            # Exclude zones, where transaction is already created
            extracted_zones = zones
            if worker_input.context is not None:
                extracted_zones = [zone for zone in zones if zone not in worker_input.context.keys()]

            # # Execute UC_TX_start subworkflow for zones, where transaction is not created
            # dynamic_tasks = []
            # dynamic_tasks_i = {}
            # for task_ref in range(0, len(extracted_zones)):
            #     dynamic_tasks.append(
            #         {
            #             'name': 'sub_task',
            #             'taskReferenceName': str(task_ref),
            #             'type': 'SUB_WORKFLOW',
            #             'subWorkflowParam': {'name': 'UC_TX_start', 'version': 2},
            #         }
            #     )
            #     dynamic_tasks_i[str(task_ref)] = dict(
            #         uniconfig_url_base=extracted_zones[task_ref]
            #     )

            return TaskResult(
                status=self.TaskResultStatus.COMPLETED,
                logs=['Dynamic fork generator worker invoked successfully'],
                output=self.WorkerOutput(
                    count=len(extracted_zones),
                    zones=extracted_zones
                )
            )

    class TxCloseFailed(WorkerImpl):
        from frinx.common.conductor_enums import TaskResultStatus
        from frinx.common.frinx_rest import CONDUCTOR_URL_BASE
        from frinx.common.frinx_rest import CONDUCTOR_HEADERS

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: str = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_find_wf'
            description: str = 'Convert context to dynamic fork input'
            labels: list[str] = ["BASICS", "UNICONFIG", "TX"]

        class WorkerInput(TaskInput):
            workflow_id: str

        class WorkerOutput(TaskOutput):
            dynamic_tasks: ListAny
            dynamic_tasks_i: DictAny

        @classmethod
        def _get_workflow(cls, workflow_id: str) -> requests.Response:
            return requests.get(
                url=cls.CONDUCTOR_URL_BASE + "/workflow/" + workflow_id,
                headers=cls.CONDUCTOR_HEADERS
            )

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            response = self._get_workflow(workflow_id=worker_input.workflow_id)

            if response.ok:
                wf_id = response.json()

                dynamic_tasks = []
                dynamic_tasks_i = {}

                reference_id = 0
                for task in wf_id['tasks']:
                    if task['taskType'] == 'SUB_WORKFLOW':
                        if task['inputData']['subWorkflowName'] == 'UC_TX_start':
                            if task['outputData']['transaction_id'] is not None:
                                dynamic_tasks.append(
                                    {
                                        'name': 'sub_task',
                                        'taskReferenceName': str(reference_id),
                                        'type': 'SUB_WORKFLOW',
                                        'subWorkflowParam': {'name': 'UC_TX_close', 'version': 2},
                                    }
                                )
                                dynamic_tasks_i[str(reference_id)] = dict(
                                    transaction_id=task['outputData']['transaction_id'],
                                    uniconfig_server_id=task['outputData']['uniconfig_server_id'],
                                    uniconfig_url_base=task['outputData']['uniconfig_url_base'],
                                )
                                reference_id += 1

                return TaskResult(
                    status=self.TaskResultStatus.COMPLETED,
                    logs=['Dynamic fork generator worker invoked successfully'],
                    output=self.WorkerOutput(
                        dynamic_tasks_i=dynamic_tasks_i,
                        dynamic_tasks=dynamic_tasks
                    )
                )

    class FindStartedTransactions(WorkerImpl):

        from frinx.common.frinx_rest import CONDUCTOR_URL_BASE
        from frinx.common.frinx_rest import CONDUCTOR_HEADERS

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_tx_find_started'
            description: str = 'Find all started UC transaction in a failed workflow'
            labels: list[str] = ["BASICS", "UNICONFIG", "TX"]

        class WorkerInput(TaskInput):
            failed_wf_id: str

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            response = requests.get(
                url=self.CONDUCTOR_URL_BASE + "/workflow/" + worker_input.failed_wf_id,
                headers=self.CONDUCTOR_HEADERS
            )
            response.json()

            if response.ok:
                response_json: dict = json.loads(response.content.decode('utf8'))

                opened_contexts = []
                committed_contexts = []

                for task in response_json.get("tasks", []):
                    # If is a subworkflow task executing UC_TX_start
                    if not task.get("inputData", {}).get("subWorkflowName", "") in ["UC_TX_start"]:
                        continue
                    # And contains started_by_wf equal to failed_wf
                    if (
                            task.get("outputData", {}).get("started_by_wf")
                            != worker_input.failed_wf_id
                    ):
                        continue

                    opened_contexts.append(task["outputData"])

                for task in response_json.get("tasks", []):
                    # If is a subworkflow task executing UC_TX_commit
                    if not task.get("inputData", {}).get("subWorkflowName", "") in [
                        "UC_TX_commit",
                        "Commit_w_decision",
                    ]:
                        continue
                    # And contains started_by_wf equal to failed_wf
                    if (
                            task.get("outputData", {}).get("committed").get("started_by_wf")
                            != worker_input.failed_wf_id
                    ):
                        continue

                    committed_contexts.append(task["outputData"]["committed"])

                return handle_response(
                    response,
                    self.WorkerOutput(
                        output=dict(
                            open=opened_contexts,
                            commited=committed_contexts
                        )
                    )
                )
