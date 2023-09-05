import json
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any
from typing import cast

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import ListAny

Keys = Iterable[str | int]


def parse_items(raw: Any | Iterable[Any], /, *, sep: str = ',') -> list[Any]:
    """
    Used for parsing workflow input where a single or multiple items are allowed.
    Doesn't change non-string inputs except wrapping them (if needed) into a list.
        Args:
            raw: a JSON string or a simple delimiter-separated string or any type
            sep: a delimiter

        Returns:
            items: a list of parsed items
    """

    if isinstance(raw, str):
        raw = raw.strip().strip(sep)

        try:
            items = json.loads(raw)

            if not isinstance(items, list):
                return [items]

            return items

        except json.JSONDecodeError:
            if sep not in raw:
                return [raw]

            items = []

            for x in raw.split(sep):
                items.extend(parse_items(x))

            return cast(list[Any], items)

    elif isinstance(raw, Mapping):
        return [raw]

    elif isinstance(raw, Iterable):
        return list(raw)

    else:
        return [raw]


def _get_value_from_path(obj: DictAny | ListAny, path: Keys, optional: bool = True, default: Any = None) -> Any:
    try:
        for key in path:
            value = obj[key]  # type: ignore
            obj = value
        return obj
    except (ValueError, LookupError):
        if optional:
            return default
        raise


def get_mandatory(obj: DictAny | ListAny, path: Keys) -> Any:
    """Returns value at given path in a nested dict/list. Raises exception if path is invalid."""
    return _get_value_from_path(obj, path, optional=False)


def get_optional(obj: DictAny | ListAny, path: Keys, default: Any = None) -> Any:
    """Returns value at given path in a nested dict/list or default if path is invalid."""
    return _get_value_from_path(obj, path, optional=True, default=default)


def finalize_response(status: str, response_body: Any) -> DictAny:
    return {'status': status, 'output': response_body if response_body else {}}


def failed_response(response_body: Any = None, error_message: bool = True) -> DictAny:
    """
    Use for workflow responses with FAILED status. Response_body should contain error message.

        Args:
            response_body (obj): anything we want to return from a workflow
            error_message (bool): if error message == True show response body under "error_message" key

        Return:
            response dictionary: {"status": "FAILED", "output": <response_body>}

    """
    if error_message:
        return finalize_response(TaskResultStatus.FAILED, {'error_message': response_body})
    return finalize_response(TaskResultStatus.FAILED, response_body)


def completed_response(response_body: Any = None) -> DictAny:
    """
    Use for workflow responses with COMPLETED status.

        Args:
            response_body (obj): anything we want to return from a workflow

        Return:
            response dictionary: {"status": "COMPLETED", "output": <response_body>}

    """
    return finalize_response(TaskResultStatus.COMPLETED, response_body)
