from datetime import datetime

from frinx.common.conductor_enums import TaskResultStatus
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
from influxdb_client.client.write_api import SYNCHRONOUS

from .utils import InfluxDbWrapper

# TODO cache for InfluxDB Client


class InfluxDB(ServiceWorkersImpl):
    class InfluxWriteData(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INFLUX_write_data"
            description: str = "Write data do InfluxDB"
            labels: ListStr = ["INFLUX"]
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
            with InfluxDbWrapper(worker_input.token, worker_input.org).client() as client:
                api = client.write_api(write_options=SYNCHRONOUS)
                api.write(
                    worker_input.bucket,
                    worker_input.org,
                    record=dict(
                        measurement=worker_input.measurement,
                        tags=worker_input.tags,
                        fields=worker_input.fields,
                        time=datetime.utcnow(),
                    ),
                )
                return TaskResult(status=TaskResultStatus.COMPLETED, logs="Successfully stored in database.")

    class InfluxQueryData(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INFLUX_query_data"
            description: str = "Query data from InfluxDB"
            labels: ListStr = ["INFLUX"]
            timeout_seconds: int = 300

        class WorkerInput(TaskInput):
            org: str
            token: str
            query: str
            format_data: ListStr | None = None

        class WorkerOutput(TaskOutput):
            data: ListAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            default_format = ["table", "_measurement", "_value", "host"]

            with InfluxDbWrapper(worker_input.token, worker_input.org).client() as client:
                query_result = client.query_api().query(worker_input.query)
                formatted_data = query_result.to_values(worker_input.format_data or default_format)

                return TaskResult(
                    status=TaskResultStatus.COMPLETED,
                    output=self.WorkerOutput(data=formatted_data),
                    logs="Data was successfully requested from database.",
                )

    class InfluxCreateBucket(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INFLUX_create_bucket"
            description: str = "Create bucket for organization in InfluxDB"
            labels: ListStr = ["INFLUX"]
            timeout_seconds: int = 300

        class WorkerInput(TaskInput):
            org: str
            token: str
            bucket: str

        class WorkerOutput(TaskOutput):
            pass

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            with InfluxDbWrapper(token=worker_input.token, org=worker_input.org).client() as client:
                api = client.buckets_api()

                if not api.find_bucket_by_name(worker_input.bucket):
                    api.create_bucket(bucket_name=worker_input.bucket, org=worker_input.org)
                    msg = "Bucket was successfully created."
                else:
                    msg = "Bucket already exists."

                return TaskResult(
                    status=TaskResultStatus.COMPLETED,
                    logs=msg,
                )
