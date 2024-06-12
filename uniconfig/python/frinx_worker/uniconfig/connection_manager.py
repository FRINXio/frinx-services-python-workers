import urllib.parse
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
from frinx_api.uniconfig.connection.manager import MountType

from . import class_to_json
from . import handle_response
from . import snake_to_kebab_case


class ConnectionManager(ServiceWorkersImpl):
    class InstallNode(WorkerImpl):
        from frinx_api.uniconfig.rest_api import InstallNode as UniconfigApi
        # from frinx_api.uniconfig.connection.manager.installnode import Cli
        # from frinx_api.uniconfig.connection.manager.installnode import Gnmi
        # from frinx_api.uniconfig.connection.manager.installnode import Netconf

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Install_node_RPC"
            description: str = "Install node to Uniconfig"

        class WorkerInput(TaskInput):
            node_id: str
            connection_type: Literal["netconf", "cli", "gnmi"]
            install_params: DictAny
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f"Failed to create request {self.UniconfigApi.request}")

            import json
            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                # FIXME prepare input with credentials (TEMPORARY SOLUTION)
                data=json.dumps(snake_to_kebab_case(self._prepare_input(worker_input))),
                # data=class_to_json(
                #     self.UniconfigApi.request(
                #         input=self.Input(
                #             node_id=worker_input.node_id,
                #             cli=self.Cli(**worker_input.install_params)
                #             if worker_input.connection_type == "cli"
                #             else None,
                #             netconf=self.Netconf(**worker_input.install_params)
                #             if worker_input.connection_type == "netconf"
                #             else None,
                #             gnmi=self.Gnmi(**worker_input.install_params)
                #             if worker_input.connection_type == "gnmi"
                #             else None,
                #         ),
                #     ),
                # ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )

            return handle_response(response, self.WorkerOutput)

        def _prepare_input(self, worker_input: WorkerInput) -> DictAny:
            if worker_input.connection_type == "cli":
                return {
                    "input": {
                        "node_id": worker_input.node_id,
                        "cli": worker_input.install_params
                    }
                }
            elif worker_input.connection_type == "netconf":
                return {
                    "input": {
                        "node_id": worker_input.node_id,
                        "netconf": worker_input.install_params
                    }
                }
            elif worker_input.connection_type == "gnmi":
                return {
                    "input": {
                        "node_id": worker_input.node_id,
                        "gnmi": worker_input.install_params
                    }
                }
            else:
                raise ValueError(f"Unknown connection type '{worker_input.connection_type}'")


    class UninstallNode(WorkerImpl):
        from frinx_api.uniconfig.connection.manager import ConnectionType
        from frinx_api.uniconfig.connection.manager.uninstallnode import Input
        from frinx_api.uniconfig.rest_api import UninstallNode as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Uninstall_node_RPC"
            description: str = "Uninstall node from Uniconfig"

        class WorkerInput(TaskInput):
            node_id: str
            connection_type: Literal["netconf", "cli", "gnmi"]
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
                            node_id=worker_input.node_id,
                            connection_type=self.ConnectionType(worker_input.connection_type),
                        ),
                    ),
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )

            return handle_response(response, self.WorkerOutput)

    class InstallMultipleNodes(WorkerImpl):
        from frinx_api.uniconfig.connection.manager.installmultiplenodes import Input
        from frinx_api.uniconfig.connection.manager.installmultiplenodes import Node
        from frinx_api.uniconfig.rest_api import InstallMultipleNodes as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Install_multiple_nodes_RPC"
            description: str = "Install nodes to Uniconfig"

        class WorkerInput(TaskInput):
            nodes: list[DictAny]
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f"Failed to create request {self.UniconfigApi.request}")

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
                headers=dict(UNICONFIG_HEADERS, accept="application/json"),
            )
            return handle_response(response, self.WorkerOutput)

    class UninstallMultipleNodes(WorkerImpl):
        from frinx_api.uniconfig.connection.manager.uninstallmultiplenodes import Input
        from frinx_api.uniconfig.connection.manager.uninstallmultiplenodes import Node
        from frinx_api.uniconfig.rest_api import UninstallMultipleNodes as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Uninstall_multiple_nodes_RPC"
            description: str = "Uninstall nodes from Uniconfig"

        class WorkerInput(TaskInput):
            nodes: list[DictAny]
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f"Failed to create request {self.UniconfigApi.request}")

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
                params=UNICONFIG_REQUEST_PARAMS,
            )

            return handle_response(response, self.WorkerOutput)

    class CheckInstalledNodes(WorkerImpl):
        from frinx_api.uniconfig.connection.manager.checkinstallednodes import Input
        from frinx_api.uniconfig.connection.manager.checkinstallednodes import TargetNodes
        from frinx_api.uniconfig.rest_api import CheckInstalledNodes as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Check_installed_nodes_RPC"
            description: str = "Check list of installed nodes in Uniconfig"

        class WorkerInput(TaskInput):
            uniconfig_url_base: str = UNICONFIG_URL_BASE
            nodes: list[str]

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if self.UniconfigApi.request is None:
                raise Exception(f"Failed to create request {self.UniconfigApi.request}")

            target_nodes = self.TargetNodes(node=worker_input.nodes)
            response = requests.request(
                url=worker_input.uniconfig_url_base + self.UniconfigApi.uri,
                method=self.UniconfigApi.method,
                data=class_to_json(
                    self.UniconfigApi.request(
                        input=self.Input(
                            target_nodes=target_nodes,
                        ),
                    ),
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )
            return handle_response(response, self.WorkerOutput)

    class GetInstalledNodes(WorkerImpl):
        from frinx_api.uniconfig.connection.manager.getinstallednodes import Input
        from frinx_api.uniconfig.rest_api import GetInstalledNodes as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Get_installed_nodes_RPC"
            description: str = "Get list of installed nodes in Uniconfig"

        class WorkerInput(TaskInput):
            uniconfig_url_base: str = UNICONFIG_URL_BASE
            mount_type: MountType = MountType.uniconfig_preferred_connection

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
                            mount_type=worker_input.mount_type,
                        ),
                    ),
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )
            return handle_response(response, self.WorkerOutput)

    class UpdateInstallParameters(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_Update_install_parameters"
            description: str = "Update install parameters of a node in Uniconfig"

        class WorkerInput(TaskInput):
            node_id: str
            """
            Identifier of the node in the UniConfig network topology.
            """
            connection_type: Literal["netconf", "cli", "gnmi"]
            """
            Type of connection to the device.
            """
            install_params: DictAny
            """
            Updated installation parameters. All installation parameters are replaced with the new values.
            If a parameter is not present in the dictionary, it is removed from the original node.
            """
            uniconfig_url_base: str = UNICONFIG_URL_BASE
            """
            Base URL of the UniConfig RESTCONF server.
            """

        class WorkerOutput(TaskOutput):
            output: DictAny
            """
            Response from the UniConfig server.
            It contains empty dictionary, if the operation was successful.
            In case of an error, it contains the error structure.
            """

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            topology_id = self._derive_topology_id(worker_input.connection_type)
            escaped_node_id = urllib.parse.quote(worker_input.node_id)
            response = requests.request(
                url=f"{worker_input.uniconfig_url_base}/data/network-topology"
                    f"/topology={topology_id}/node={escaped_node_id}",
                method="PUT",
                data=class_to_json({
                    "node": [
                        {
                            "node-id": worker_input.node_id,
                            **worker_input.install_params,
                        }
                    ]
                }
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS,
            )
            return handle_response(response, self.WorkerOutput)

        @staticmethod
        def _derive_topology_id(connection_type: Literal["netconf", "cli", "gnmi"]) -> str:
            if connection_type == "netconf":
                return "topology-netconf"
            if connection_type == "cli":
                return "cli"
            if connection_type == "gnmi":
                return "gnmi-topology"
            raise ValueError(f"Unknown connection type {connection_type}")
