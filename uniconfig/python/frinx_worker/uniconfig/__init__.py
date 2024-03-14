import dataclasses
import http.client
import json
from typing import Any
from typing import Optional

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.util import remove_empty_elements_from_dict
from frinx.common.worker.task_result import TaskResult
from pydantic import BaseModel
from requests import Response


class TransactionContext(BaseModel):
    uniconfig_server_id: str
    transaction_id: str


def handle_response(response: Response, worker_output: Any) -> TaskResult:
    """
    Handles an HTTP response from Uniconfig by logging the request details and processing the response content.

    Args:
        response: The HTTP response from Uniconfig.
        worker_output:  Typically a callable Type[WorkerOutput]. It's common for this callable to be a method like
        `self.WorkerOutput`, which can construct an instance of `WorkerOutput`.

    Returns:
        TaskResult: An object representing the result of the task. It includes the task status (COMPLETED on success,
                    FAILED otherwise), logs detailing the response, and the processed output.
    """
    output = dict()
    status = TaskResultStatus.COMPLETED if response.ok else TaskResultStatus.FAILED
    logs = (f'{response.request.method} request to {response.url} returned with status code {response.status_code}.  '
            f'CONTENT: {response.content.decode("utf-8")}')

    # Attempt to parse response content as JSON if the response contains content.
    if response.status_code != http.client.NO_CONTENT:
        try:
            output = response.json()
        except json.JSONDecodeError:
            logs += 'ERROR: JSON decoding failed - unparsable response content. '
            status = TaskResultStatus.FAILED

    return TaskResult(
        status=status,
        logs=logs,
        output=worker_output(output=output)
    )


def uniconfig_zone_to_cookie(
        transaction_id: Optional[str] = None,
        uniconfig_server_id: Optional[str] = None
) -> DictAny:
    cookies: DictAny = {}
    if transaction_id:
        cookies['UNICONFIGTXID'] = transaction_id
    if uniconfig_server_id:
        cookies['uniconfig_server_id'] = uniconfig_server_id
    return cookies


def snake_to_kebab_case(data: Any) -> Any:
    """
    Recursively converts keys from snake_case to kebab-case in a dictionary or list.

    Args:
        data (Any): The input dictionary or list to be processed.

    Returns:
        Any: A new dictionary or list with keys converted to kebab-case.
    """
    if isinstance(data, dict):
        return {k.replace('_', '-'): snake_to_kebab_case(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [snake_to_kebab_case(item) for item in data]
    else:
        return data


def class_to_json(dataclass: Any) -> Any:
    """
    Converts a DictAny or dataclass instance to a JSON string with keys in kebab-case.

    Args:
        dataclass (dataclasses.dataclass): The input dataclass instance to be converted.

    Returns:
        Any: A JSON string representation of the dataclass with keys in kebab-case.
    """
    if dataclasses.is_dataclass(dataclass):
        return json.dumps(snake_to_kebab_case(remove_empty_elements_from_dict(dataclasses.asdict(dataclass))))
    elif isinstance(dataclass, BaseModel):
        return dataclass.json(exclude_none=True, by_alias=True, exclude_defaults=True)
    else:
        return json.dumps(snake_to_kebab_case(remove_empty_elements_from_dict(dataclass)))
