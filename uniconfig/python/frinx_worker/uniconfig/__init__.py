import dataclasses
import json
from typing import Any
from typing import Optional

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.util import remove_empty_elements_from_dict
from frinx.common.worker.task_result import TO
from frinx.common.worker.task_result import TaskResult
from pydantic import BaseModel
from requests import Response


class TransactionContext(BaseModel):
    uniconfig_server_id: str
    transaction_id: str


def handle_response(response: Response, worker_output: Optional[TO] = None) -> TaskResult[TO]:
    if not response.ok:
        return TaskResult(
            status=TaskResultStatus.FAILED,
            logs=response.content.decode('utf8')
        )
    return TaskResult(
        status=TaskResultStatus.COMPLETED,
        output=worker_output
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
