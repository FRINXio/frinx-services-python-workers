from frinx.common.type_aliases import ListStr
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx_api.blueprints_provider import BlueprintNode
from frinx_api.blueprints_provider import BlueprintNodeInput
from frinx_api.blueprints_provider import BlueprintOutput
from frinx_api.blueprints_provider import BlueprintType
from frinx_api.blueprints_provider import ConnectionType
from frinx_api.blueprints_provider import CreateBlueprintMutation
from pydantic import Field

from frinx_worker.blueprint_provider.utils import BlueprintsProviderOutput
from frinx_worker.blueprint_provider.utils import BlueprintsProviderWorkerOutput
from frinx_worker.blueprint_provider.utils import execute_graphql_operation
from frinx_worker.blueprint_provider.utils import response_handler


class BlueprintsProviderWorkers(ServiceWorkersImpl):
    class CreateBlueprint(WorkerImpl):

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "BLUEPRINTS_PROVIDER_create_blueprint"
            description: str = "Create blueprint"
            labels: ListStr = ["BLUEPRINTS-PROVIDER"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            name: str = Field(
                description="Blueprint name",
                examples=["install_cli_cisco", "install_gnmi_nokia_7750_22_10"],
            )
            blueprint_type: BlueprintType = Field(
                description="Specifies the type of blueprint",
                examples=[BlueprintType.TOPOLOGY_LLDP],
            )
            connection_type: ConnectionType = Field(
                description="Management protocol used for creation of connection to the device",
                examples=[ConnectionType.NETCONF],
            )
            model_pattern: str | None = Field(
                description="Regular expression pattern for matching the model of the device",
                examples=["vsr .+"],
            )
            vendor_pattern: str | None = Field(
                description="Regular expression pattern for matching the vendor of the device",
                examples=[".*wrt"],
            )
            version_pattern: str | None = Field(
                description="Regular expression pattern for matching the version of the devices",
                examples=["v[0-9]+"],
            )
            template: str = Field(description="JSON string")


        class WorkerOutput(BlueprintsProviderWorkerOutput):
            ...

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            create_blueprint_mutation = CreateBlueprintMutation(
                node=BlueprintNodeInput(
                    name=worker_input.name,
                    blueprint_type=worker_input.blueprint_type,
                    connection_type=worker_input.connection_type,
                    model_pattern=worker_input.model_pattern,
                    vendor_pattern=worker_input.vendor_pattern,
                    version_pattern=worker_input.version_pattern,
                    template=worker_input.template,
                ),
                payload=BlueprintOutput(
                    id=True,
                    node=BlueprintNode(
                        name=True,
                        blueprint_type=True,
                        connection_type=True,
                        model_pattern=True,
                        vendor_pattern=True,
                        version_pattern=True,
                        template=True,
                    )
                )
            )

            query = create_blueprint_mutation.render()
            response: BlueprintsProviderOutput = execute_graphql_operation(query=query.query, variables=query.variable)
            return response_handler(query, response)
