import json

from typing import Any
from typing import Optional

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task import Task
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import ListStr

from .utils import InfluxOutput, InfluxDbWrapper


class Influx(ServiceWorkersImpl):
    class InfluxWriteData(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name = "INFLUX_write_data"
            description = "Write data do InfluxDB"
            labels = ["INFLUX"]
            timeout_seconds = 300

        class WorkerInput(TaskInput):
            org: str
            token: str
            bucket: str
            measurement: str
            tags: DictAny | str
            fields: DictAny | str

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:



            response = influxdb_worker.influx_write_data(**task.input_data)
            return response_handler(response)

    ###############################################################################
    class InfluxQueryData(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name = "INFLUX_query_data"
            description = "Query data from InfluxDB"
            labels = ["INFLUX"]
            timeout_seconds = 300

        class WorkerInput(TaskInput):
            org: str
            token: str
            query: str
            format_data: Optional[ListStr] | Optional[str]

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            if worker_input.format_data is not None:
                format_data = worker_input.format_data
                if isinstance(format_data, str):
                    format_data = list(worker_input.format_data.replace(" ", "").split(","))
            else:
                format_data = ["table", "_measurement", "_value", "host"]

            response = InfluxOutput(code=404, data={}, logs=None)

            with InfluxDbWrapper(token=worker_input.token, org=worker_input.org).client() as client:
                output = (
                    client.query_api().query(json.loads(json.dumps(worker_input.query))).to_values(columns=format_data)
                )

                client.close()
                response.data["output"] = json.loads(json.dumps(output))
                response.code = 200

            return response_handler(response)

    ###############################################################################
    class InfluxCreateBucket(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name = "INFLUX_create_bucket"
            description = "Create bucket for organization in InfluxDB"
            labels = ["INFLUX"]
            timeout_seconds = 300

        class WorkerInput(TaskInput):
            org: str
            token: str
            bucket: str

        class WorkerOutput(TaskOutput):
            code: DictAny
            data: DictAny
            logs: Optional[str]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            response = influxdb_worker.influx_create_bucket(**task.input_data)
            return response_handler(response)


def response_handler(response: InfluxOutput) -> TaskResult:
    match response.code:
        case 200 | 201:
            task_result = TaskResult(status=TaskResultStatus.COMPLETED)
            if response.code:
                task_result.add_output_data("response_code", response.code)
            if response.data:
                task_result.add_output_data("response_body", response.data)
            if response.logs:
                task_result.logs = response.logs

            return task_result
        case _:
            task_result = TaskResult(status=TaskResultStatus.FAILED)
            task_result.status = TaskResultStatus.FAILED
            task_result.logs = task_result.logs or str(response)
            if response.code:
                task_result.add_output_data("response_code", response.code)
            if response.data:
                task_result.add_output_data("response_body", response.data)
            return task_result