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
from frinx.common.worker.worker import WorkerImpl

from . import class_to_json
from . import handle_response
from . import uniconfig_zone_to_cookie


class SnapshotManager(ServiceWorkersImpl):
    class CreateSnapshot(WorkerImpl):
        from frinx_api.uniconfig.rest_api import CreateSnapshot as UniconfigApi
        from frinx_api.uniconfig.snapshot.manager.createsnapshot import Input
        from frinx_api.uniconfig.snapshot.manager.createsnapshot import TargetNodes

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Create_snapshot_RPC"
            description: str = "Create Uniconfig snapshot"

        class WorkerInput(TaskInput):
            node_ids: list[str]
            snapshot_name: str
            transaction_id: str | None = None
            uniconfig_server_id: str | None = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f"Failed to create request {self.UniconfigApi.request}")

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            name=worker_input.snapshot_name,
                            target_nodes=self.TargetNodes(node=worker_input.node_ids),
                        )
                    )
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id, transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )

            return handle_response(response, self.WorkerOutput)

    class DeleteSnapshot(WorkerImpl):
        from frinx_api.uniconfig.rest_api import DeleteSnapshot as UniconfigApi
        from frinx_api.uniconfig.snapshot.manager.deletesnapshot import Input

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Delete_snapshot_RPC"
            description: str = "Delete Uniconfig snapshot"

        class WorkerInput(TaskInput):
            snapshot_name: str
            transaction_id: str
            uniconfig_server_id: str | None = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f"Failed to create request {self.UniconfigApi.request}")

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            name=worker_input.snapshot_name,
                        )
                    )
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id, transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )

            return handle_response(response, self.WorkerOutput)

    class ReplaceConfigWithSnapshot(WorkerImpl):
        from frinx_api.uniconfig.rest_api import ReplaceConfigWithSnapshot as UniconfigApi
        from frinx_api.uniconfig.snapshot.manager.replaceconfigwithsnapshot import Input
        from frinx_api.uniconfig.snapshot.manager.replaceconfigwithsnapshot import TargetNodes

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Replace_config_with_snapshot_RPC"
            description: str = "Replace Uniconfig CONFIG datastore with a snapshot"

        class WorkerInput(TaskInput):
            snapshot_name: str
            node_ids: list[str]
            transaction_id: str
            uniconfig_server_id: str | None = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f"Failed to create request {self.UniconfigApi.request}")

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            name=worker_input.snapshot_name,
                            target_nodes=self.TargetNodes(node=worker_input.node_ids),
                        )
                    )
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id, transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )

            return handle_response(response, self.WorkerOutput)
