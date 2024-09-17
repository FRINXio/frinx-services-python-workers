from frinx.common.type_aliases import ListStr
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx_api.topology_discovery import SyncMutation
from frinx_api.topology_discovery import SyncResponse
from frinx_api.topology_discovery import TopologyType
from pydantic import Field

from frinx_worker.topology_discovery.utils import TopologyOutput
from frinx_worker.topology_discovery.utils import TopologyWorkerOutput
from frinx_worker.topology_discovery.utils import execute_graphql_operation
from frinx_worker.topology_discovery.utils import response_handler


class TopologyDiscoveryWorkers(ServiceWorkersImpl):
    class TopologySync(WorkerImpl):
        _sync_topology: SyncMutation = SyncMutation(
            topologyType=TopologyType.PHYSICAL_TOPOLOGY,
            devices=["dev1"],
            labels=["label1"],
            payload=SyncResponse(
                labels=True,
                loadedDevices=True,
                devicesMissingInInventory=True,
                devicesMissingInUniconfig=True,
            )
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "TOPOLOGY_sync"
            description: str = "Synchronize specified devices in the topology"
            labels: ListStr = ["BASIC", "TOPOLOGY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            devices: ListStr = Field(
                description="List of device identifiers that should be synchronized",
                examples=[["device_id_1", "device_id_2"]],
            )
            topololy: TopologyType = Field(
                description="To be synchronized topology type",
                examples=[TopologyType.ETH_TOPOLOGY],
            )
            labels: ListStr | None = Field(
                description="List of labels that are assigned to the synchronized devices",
                examples=[["label_1", "label_2"]],
            )

        class WorkerOutput(TopologyWorkerOutput):
            ...

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self._sync_topology.topology_type = worker_input.topololy
            self._sync_topology.devices = worker_input.devices
            self._sync_topology.labels = worker_input.labels

            query = self._sync_topology.render()
            response: TopologyOutput = execute_graphql_operation(query=query.query, variables=query.variable)
            return response_handler(query, response)
