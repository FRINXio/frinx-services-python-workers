import requests
from frinx.common.frinx_rest import UNICONFIG_HEADERS
from frinx.common.frinx_rest import UNICONFIG_REQUEST_PARAMS
from frinx.common.frinx_rest import UNICONFIG_URL_BASE
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx_api.uniconfig import OperationsFindSchemaResourcesPostResponse

from . import class_to_json
from . import handle_response
from . import uniconfig_zone_to_cookie


class SchemaResourcesWorkers(ServiceWorkersImpl):
    class FindSchemaResources(WorkerImpl):
        from frinx_api.uniconfig.rest_api import FindSchemaResources as UniconfigApi
        from frinx_api.uniconfig.schema.resources.findschemaresources import Input

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Find_schema_resources_RPC"
            description: str = "Find schema resources"

        class WorkerInput(TaskInput):
            repository_name: str
            keywords: list[str]
            transaction_id: str | None = None
            uniconfig_server_id: str | None = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: OperationsFindSchemaResourcesPostResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f"Failed to create request {self.UniconfigApi.request}")

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            repository_name=worker_input.repository_name,
                            keywords=worker_input.keywords,
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

