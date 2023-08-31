from typing import Any
from typing import Optional

import requests
from frinx.common.frinx_rest import UNICONFIG_HEADERS
from frinx.common.frinx_rest import UNICONFIG_REQUEST_PARAMS
from frinx.common.frinx_rest import UNICONFIG_URL_BASE
from frinx.common.type_aliases import ListAny
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl

from . import class_to_json
from . import handle_response
from . import uniconfig_zone_to_cookie


class CliNetworkTopology(ServiceWorkersImpl):

    class ExecuteAndRead(WorkerImpl):

        from frinx_api.uniconfig.cli.unit.generic.executeandread import Input
        from frinx_api.uniconfig.rest_api import ExecuteAndRead as UniconfigApi

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Execute_and_read_RPC'
            description: str = 'Run execute and read RPC'

        class WorkerInput(TaskInput):
            node_id: str
            topology_id: str = 'uniconfig'
            command: str
            wait_for_output: int = 0
            error_check: bool = True
            transaction_id: Optional[str]
            uniconfig_server_id: Optional[str]
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: dict[str, Any]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri.format(
                    tid=worker_input.topology_id, nid=worker_input.node_id
                ),
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            wait_for_output_timer=worker_input.wait_for_output,
                            error_check=worker_input.error_check,
                            command=worker_input.command,
                        ),
                    ),
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )

            return handle_response(response, self.WorkerOutput(output=response.json()))

    class Execute(WorkerImpl):

        from frinx_api.uniconfig.cli.unit.generic.execute import Input
        from frinx_api.uniconfig.rest_api import Execute as UniconfigApi

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Execute_RPC'
            description: str = 'Run execute RPC'
            labels: ListAny = ['UNICONFIG']

        class WorkerInput(TaskInput):
            node_id: str
            topology_id: str = 'uniconfig'
            command: str
            wait_for_output: int = 0
            error_check: bool = True
            transaction_id: Optional[str]
            uniconfig_server_id: Optional[str]
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: dict[str, Any]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri.format(
                    tid=worker_input.topology_id, nid=worker_input.node_id
                ),
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            wait_for_output_timer=worker_input.wait_for_output,
                            error_check=worker_input.error_check,
                            command=worker_input.command,
                        ),
                    ),
                ),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            return handle_response(response, self.WorkerOutput(output=response.json()))
