from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import DictStr
from frinx.common.type_aliases import ListAny
from frinx.common.util import parse_response
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from pydantic import AnyHttpUrl
from pydantic import NonNegativeFloat
from requests import request
from requests.auth import HTTPBasicAuth

from .enums import ContentType
from .enums import HttpMethod


class HTTPWorkersService(ServiceWorkersImpl):
    class GenericHTTPWorker(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = "HTTP_task"
            description: str = "Generic http task"
            labels: ListAny = ["SIMPLE"]
            timeout_seconds: int = 360
            response_timeout_seconds: int = 360

        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True
            exclude_empty_inputs: bool = True

        class WorkerInput(TaskInput):
            url: AnyHttpUrl
            method: HttpMethod
            headers: DictStr = {}
            cookies: DictStr = {}
            connect_timeout: NonNegativeFloat = 360
            content_type: ContentType | None = None
            basic_auth_name: str | None = None
            basic_auth_passwd: str | None = None
            body: DictAny | ListAny | str | None = None
            read_timeout: NonNegativeFloat | None = None

            model_config = TaskInput.model_config
            model_config["use_enum_values"] = True

        class WorkerOutput(TaskOutput):
            status_code: int
            response: DictAny | ListAny | str
            cookies: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if worker_input.content_type:
                worker_input.headers["Content-Type"] = worker_input.content_type

            data_body, json_doby = None, None
            match worker_input.body:
                case dict() | list():
                    json_doby = worker_input.body
                case str():
                    data_body = worker_input.body

            auth = None
            if worker_input.basic_auth_name and worker_input.basic_auth_passwd:
                auth = HTTPBasicAuth(username=worker_input.basic_auth_name, password=worker_input.basic_auth_passwd)
            elif worker_input.basic_auth_name or worker_input.basic_auth_passwd:
                raise Exception("Missing basic_auth_name or basic_auth_passwd")

            timeout: float | tuple[float, float] = worker_input.connect_timeout
            if worker_input.read_timeout:
                timeout = (worker_input.connect_timeout, worker_input.read_timeout)

            response = request(
                url=str(worker_input.url),
                timeout=timeout,
                method=worker_input.method,
                cookies=worker_input.cookies,
                headers=worker_input.headers,
                auth=auth,
                json=json_doby,
                data=data_body,
            )

            if not response.ok:
                raise Exception(response.reason)

            cookies: DictAny = response.cookies.get_dict()  # type: ignore[no-untyped-call]

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    status_code=response.status_code,
                    cookies=cookies,
                    response=parse_response(response=response),
                ),
                logs=f"{worker_input.method} {worker_input.url} {response.status_code} {response.reason}",
            )
