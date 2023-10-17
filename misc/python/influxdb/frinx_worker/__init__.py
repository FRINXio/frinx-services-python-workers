from typing import Optional

from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import ListAny
from frinx.common.type_aliases import ListStr
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl

from . import utils


class Influx(ServiceWorkersImpl):
    
    class InfluxWriteData(WorkerImpl):

        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INFLUX_write_data'
            description: str = 'Write data do InfluxDB'
            labels: ListStr = ['INFLUX']
            timeout_seconds: int = 300

        class WorkerInput(TaskInput):
            org: str
            token: str
            bucket: str
            measurement: str
            tags: DictAny
            fields: DictAny

        class WorkerOutput(TaskOutput):
            pass

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            return utils.influx_write_data(self, worker_input)

    class InfluxQueryData(WorkerImpl):

        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INFLUX_query_data'
            description: str = 'Query data from InfluxDB'
            labels: ListStr = ['INFLUX']
            timeout_seconds: int = 300

        class WorkerInput(TaskInput):
            org: str
            token: str
            query: str
            format_data: Optional[ListStr] = None

        class WorkerOutput(TaskOutput):
            data: ListAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            return utils.influx_query_data(self, worker_input)

    class InfluxCreateBucket(WorkerImpl):

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INFLUX_create_bucket'
            description: str = 'Create bucket for organization in InfluxDB'
            labels: ListStr = ['INFLUX']
            timeout_seconds: int = 300

        class WorkerInput(TaskInput):
            org: str
            token: str
            bucket: str

        class WorkerOutput(TaskOutput):
            pass

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            return utils.influx_create_bucket(self, worker_input)
