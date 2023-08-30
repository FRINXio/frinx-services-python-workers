from typing import Any

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import DictStr
from frinx.common.type_aliases import ListAny
from frinx.common.type_aliases import ListStr
from frinx.common.util import parse_response
from frinx.common.util import snake_to_camel_case
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from pydantic import AnyHttpUrl
from pydantic import NonNegativeFloat

from .enums import ContentType
from .enums import HttpMethod
from .utils import BasicAuth
from .utils import http_task


class HTTPWorkersService(ServiceWorkersImpl):
    class GenericHTTPWorker(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = 'HTTP_task'
            description: str = 'Generic http task'
            labels: ListAny = ['SIMPLE']
            timeout_seconds: int = 360
            response_timeout_seconds: int = 360

        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True
            exclude_empty_inputs: bool = True

        class WorkerInput(TaskInput):
            uri: AnyHttpUrl
            method: HttpMethod
            basic_auth: BasicAuth | None = None
            content_type: ContentType | None = None
            body: DictAny | ListAny | str | None = None
            read_timeout: NonNegativeFloat | None = None
            connect_timeout: NonNegativeFloat = 360
            headers: DictStr | None = {}
            cookies: DictStr | None = {}

            class Config(TaskInput.Config):
                alias_generator = snake_to_camel_case
                allow_population_by_field_name = True
                arbitrary_types_allowed = True
                use_enum_values = True

        class WorkerOutput(TaskOutput):
            status_code: int
            response: DictAny | ListAny | str
            cookies: DictAny
            logs: ListStr | str

            class Config(TaskOutput.Config):
                alias_generator = snake_to_camel_case
                allow_population_by_field_name = True
                min_anystr_length = 1

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            response = http_task(worker_input)
            logs = f'{worker_input.method} {worker_input.uri} {response.status_code} {response.reason}'

            return TaskResult(
                logs=logs,
                status=TaskResultStatus.COMPLETED if response.ok else TaskResultStatus.FAILED,
                output=self.WorkerOutput(
                    status_code=response.status_code,
                    response=parse_response(response),
                    cookies=response.cookies.get_dict(),
                    logs=logs
                )
            )