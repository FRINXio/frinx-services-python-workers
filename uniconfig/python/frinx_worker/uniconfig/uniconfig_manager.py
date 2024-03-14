from typing import Optional

import requests
from frinx.common.frinx_rest import UNICONFIG_HEADERS
from frinx.common.frinx_rest import UNICONFIG_REQUEST_PARAMS
from frinx.common.frinx_rest import UNICONFIG_URL_BASE
from frinx.common.type_aliases import DictAny
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.task_result import TaskResultStatus
from frinx.common.worker.worker import WorkerImpl

from . import class_to_json
from . import handle_response
from . import uniconfig_zone_to_cookie


class UniconfigManager(ServiceWorkersImpl):
    class CreateTransaction(WorkerImpl):
        from frinx_api.uniconfig.rest_api import CreateTransaction as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Create_transaction_RPC'
            description: str = 'Create Uniconfig transaction'

        class WorkerInput(TaskInput):
            transaction_timeout: Optional[int] = None
            use_dedicated_session: bool = False
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            transaction_id: Optional[str] = None
            uniconfig_server_id: Optional[str] = None
            uniconfig_url_base: str

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(self.UniconfigApi.request()),
                headers=dict(UNICONFIG_HEADERS)
            )

            if not response.ok:
                return TaskResult(
                    status=TaskResultStatus.FAILED,
                    logs=response.content.decode('utf8'),
                    output=self.WorkerOutput(
                        uniconfig_url_base=worker_input.uniconfig_url_base
                    )
                )
            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    transaction_id=response.cookies['UNICONFIGTXID'],
                    uniconfig_server_id=response.cookies['uniconfig_server_id'],
                    uniconfig_url_base=worker_input.uniconfig_url_base
                )
            )

    class CloseTransaction(WorkerImpl):
        from frinx_api.uniconfig.rest_api import CloseTransaction as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Close_transaction_RPC'
            description: str = 'Close Uniconfig transaction'

        class WorkerInput(TaskInput):
            uniconfig_url_base: str = UNICONFIG_URL_BASE
            uniconfig_server_id: Optional[str] = None
            transaction_id: Optional[str] = None

        class WorkerOutput(TaskOutput):
            closed: Optional[DictAny] = {}
            unclosed: Optional[DictAny] = {}

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                    )
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id,
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            if not response.ok:
                return TaskResult(
                    status=TaskResultStatus.FAILED,
                    logs=response.content.decode('utf8'),
                    output=self.WorkerOutput(
                        unclosed=dict(
                            transaction_id=worker_input.transaction_id,
                            uniconfig_server_id=worker_input.uniconfig_server_id,
                        )
                    )
                )
            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    closed=dict(
                        transaction_id=worker_input.transaction_id,
                        uniconfig_server_id=worker_input.uniconfig_server_id,
                    )
                )
            )

    class CommitTransaction(WorkerImpl):
        from frinx_api.uniconfig.rest_api import Commit as UniconfigApi
        from frinx_api.uniconfig.uniconfig.manager.commit import Input as CommitInput

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Commit_transaction_RPC'
            description: str = 'Commit Uniconfig transaction'

        class WorkerInput(TaskInput):
            confirmed_commit: bool = False
            validate_commit: bool = True
            rollback: Optional[bool] = None
            skip_unreachable_nodes: Optional[bool] = None
            transaction_id: Optional[str] = None
            uniconfig_server_id: Optional[str] = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.CommitInput(
                            do_confirmed_commit=worker_input.confirmed_commit,
                            do_validate=worker_input.validate_commit,
                            do_rollback=worker_input.rollback,
                            skip_unreachable_nodes=worker_input.skip_unreachable_nodes,
                        )
                    )
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            return handle_response(response, self.WorkerOutput)

    class ReplaceConfigWithOperational(WorkerImpl):
        from frinx_api.uniconfig.rest_api import ReplaceConfigWithOperational as UniconfigApi
        from frinx_api.uniconfig.uniconfig.manager.replaceconfigwithoperational import Input
        from frinx_api.uniconfig.uniconfig.manager.replaceconfigwithoperational import TargetNodes

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Replace_config_with_operational_RPC'
            description: str = 'Replace Uniconfig CONFIG datastore with operational datastore'

        class WorkerInput(TaskInput):
            node_ids: list[str]
            transaction_id: str
            uniconfig_server_id: Optional[str] = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            target_nodes=self.TargetNodes(node=worker_input.node_ids),
                        )
                    )
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            return handle_response(response, self.WorkerOutput)

    class SyncFromNetwork(WorkerImpl):
        from frinx_api.uniconfig.rest_api import SyncFromNetwork as UniconfigApi
        from frinx_api.uniconfig.uniconfig.manager.syncfromnetwork import Input
        from frinx_api.uniconfig.uniconfig.manager.syncfromnetwork import TargetNodes

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Sync_from_network_RPC'
            description: str = 'Synchronize configuration from network and the UniConfig nodes'

        class WorkerInput(TaskInput):
            node_ids: list[str]
            transaction_id: str
            uniconfig_server_id: Optional[str] = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            target_nodes=self.TargetNodes(node=worker_input.node_ids),
                        )
                    )
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            return handle_response(response, self.WorkerOutput)

    class DryRunCommit(WorkerImpl):
        from frinx_api.uniconfig.dryrun.manager.dryruncommit import Input
        from frinx_api.uniconfig.dryrun.manager.dryruncommit import TargetNodes
        from frinx_api.uniconfig.rest_api import DryrunCommit as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_dryrun_commit'
            description: str = 'Dryrun Commit uniconfig'

        class WorkerInput(TaskInput):
            do_rollback: Optional[bool] = False
            node: Optional[list[str]] = None
            transaction_id: Optional[str] = None
            uniconfig_server_id: Optional[str] = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            do_rollback=worker_input.do_rollback,
                            target_nodes=self.TargetNodes(
                                node=worker_input.node
                            )
                        )
                    )
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            return handle_response(response, self.WorkerOutput)
