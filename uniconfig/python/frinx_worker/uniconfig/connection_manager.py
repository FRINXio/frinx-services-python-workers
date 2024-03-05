from typing import Literal

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


class ConnectionManager(ServiceWorkersImpl):
    class InstallNode(WorkerImpl):
        from frinx_api.uniconfig.connection.manager.installnode import Cli
        from frinx_api.uniconfig.connection.manager.installnode import Input
        from frinx_api.uniconfig.connection.manager.installnode import Netconf
        from frinx_api.uniconfig.rest_api import InstallNode as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Install_node_RPC'
            description: str = 'Install node to Uniconfig'

        class WorkerInput(TaskInput):
            node_id: str
            connection_type: Literal['netconf', 'cli']
            install_params: DictAny
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
                            node_id=worker_input.node_id,
                            cli=self.Cli(**worker_input.install_params) if
                            worker_input.connection_type == 'cli' else None,
                            netconf=self.Netconf(**worker_input.install_params) if
                            worker_input.connection_type == 'netconf' else None,
                        ),
                    ),
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            uniconfig_result = handle_response(response)

            return TaskResult(
                status=uniconfig_result.task_status,
                logs=uniconfig_result.logs,
                output=self.WorkerOutput(
                    output=uniconfig_result.output,
                ))

    class UninstallNode(WorkerImpl):
        from frinx_api.uniconfig.connection.manager import ConnectionType
        from frinx_api.uniconfig.connection.manager.uninstallnode import Input
        from frinx_api.uniconfig.rest_api import UninstallNode as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Uninstall_node_RPC'
            description: str = 'Uninstall node from Uniconfig'

        class WorkerInput(TaskInput):
            node_id: str
            connection_type: Literal['netconf', 'cli']
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
                            node_id=worker_input.node_id,
                            connection_type=self.ConnectionType(
                                worker_input.connection_type
                            )
                        ),
                    ),
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            uniconfig_result = handle_response(response)

            return TaskResult(
                status=uniconfig_result.task_status,
                logs=uniconfig_result.logs,
                output=self.WorkerOutput(
                    output=uniconfig_result.output,
                ))

    class InstallMultipleNodes(WorkerImpl):
        from frinx_api.uniconfig.connection.manager.installmultiplenodes import Input
        from frinx_api.uniconfig.connection.manager.installmultiplenodes import Node
        from frinx_api.uniconfig.rest_api import InstallMultipleNodes as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Install_multiple_nodes_RPC'
            description: str = 'Install nodes to Uniconfig'

        class WorkerInput(TaskInput):
            nodes: list[DictAny]
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            nodes = []
            for node in worker_input.nodes:
                nodes.append(self.Node(**node))

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            nodes=nodes,
                        ),
                    ),
                ),
                headers=dict(UNICONFIG_HEADERS, accept='application/json')
            )

            uniconfig_result = handle_response(response)

            return TaskResult(
                status=uniconfig_result.task_status,
                logs=uniconfig_result.logs,
                output=self.WorkerOutput(
                    output=uniconfig_result.output,
                ))

    class UninstallMultipleNodes(WorkerImpl):
        from frinx_api.uniconfig.connection.manager.uninstallmultiplenodes import Input
        from frinx_api.uniconfig.connection.manager.uninstallmultiplenodes import Node
        from frinx_api.uniconfig.rest_api import UninstallMultipleNodes as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_Uninstall_multiple_nodes_RPC'
            description: str = 'Uninstall nodes from Uniconfig'

        class WorkerInput(TaskInput):
            nodes: list[DictAny]
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f'Failed to create request {self.UniconfigApi.request}')

            nodes = []
            for node in worker_input.nodes:
                nodes.append(self.Node(**node))

            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            nodes=nodes,
                        ),
                    ),
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            uniconfig_result = handle_response(response)

            return TaskResult(
                status=uniconfig_result.task_status,
                logs=uniconfig_result.logs,
                output=self.WorkerOutput(
                    output=uniconfig_result.output,
                ))
