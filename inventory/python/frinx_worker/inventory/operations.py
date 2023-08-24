from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.frinx_rest import INVENTORY_URL_BASE
from frinx.common.type_aliases import DictAny
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx_worker.inventory import execute_inventory_query


class Inventory(ServiceWorkersImpl):
    class ExecuteInventoryQuery(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_Execute_inventory_query'
            description: str = 'Execute inventory query'

        class WorkerInput(TaskInput):
            query: str
            variables: DictAny
            inventory_url_base: str = INVENTORY_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            response = execute_inventory_query(
                query=worker_input.query,
                variables=worker_input.variables,
                inventory_url_base=worker_input.inventory_url_base
            )
            return TaskResult(status=TaskResultStatus.COMPLETED, output=self.WorkerOutput(output=response.json()))
