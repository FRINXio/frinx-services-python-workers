import os
from enum import Enum
from types import MappingProxyType

import requests
from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import ListAny
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from graphql_pydantic_converter.graphql_types import QueryForm
from pydantic import BaseModel
from pydantic import Field

# TODO: add to frinx-python-sdk
BLUEPRINTS_PROVIDER_URL_BASE = os.getenv('BLUEPRINTS_PROVIDER_URL_BASE', 'http://localhost:8080/graphql')
BLUEPRINTS_PROVIDER_HEADERS = MappingProxyType({'Content-Type': 'application/json'})


class BlueprintsProviderWorkerOutput(TaskOutput):
    """Blueprints-provider worker output."""

    query: str = Field(
        description="Constructed GraphQL query.",
    )
    variable: DictAny | None = Field(
        description="Constructed input GraphQL variables.",
        default=None
    )
    response_body: ListAny | DictAny | None | None = Field(
        description="Response body.",
    )
    response_code: int = Field(
        description="Response code.",
    )

class ResponseStatus(str, Enum):
    """Response status."""

    DATA = "data"
    """Response contains valid data."""
    ERRORS = "errors"
    """Response contains some errors."""
    FAILED = "failed"
    """HTTP request failed without providing list of errors in response."""


class BlueprintsProviderOutput(BaseModel):
    """Parsed response from Blueprints-provider service."""

    status: ResponseStatus = Field(
        description="Response status."
    )
    code: int = Field(
        description="Parsed response code."
    )
    data: ListAny | DictAny | None = Field(
        default=None,
        description="Structured response data."
    )


def execute_graphql_operation(
    query: str,
    variables: DictAny | None = None,
    blueprint_provider_url_base: str = BLUEPRINTS_PROVIDER_URL_BASE
) -> BlueprintsProviderOutput:
    """
    Execute GraphQL query.

    :param query: GraphQL query
    :param variables: GraphQL variables in dictionary format
    :param blueprint_provider_url_base: Blueprints-provider service URL base
    :return: BlueprintsProviderOutput object
    """ ""
    response = requests.post(
        blueprint_provider_url_base,
        json={
            "query": query,
            "variables": variables
        },
        headers=BLUEPRINTS_PROVIDER_HEADERS
    )
    data = response.json()
    status_code = response.status_code

    if data.get("errors") is not None:
        return BlueprintsProviderOutput(data=data["errors"], status=ResponseStatus.ERRORS, code=status_code)

    if data.get("data") is not None:
        return BlueprintsProviderOutput(data=data["data"], status=ResponseStatus.DATA, code=status_code)

    return BlueprintsProviderOutput(status=ResponseStatus.FAILED, code=status_code)


def response_handler(query: QueryForm, response: BlueprintsProviderOutput) -> TaskResult:
    """
    Handle response from Blueprints-service service.

    :param query: GraphQL query information
    :param response: parsed topology-discovery response
    :return: built TaskResult object
    """
    output = BlueprintsProviderWorkerOutput(
        response_code=response.code,
        response_body=response.data,
        query=query.query,
        variable=query.variable
    )
    match response.status:
        case ResponseStatus.DATA:
            task_result = TaskResult(status=TaskResultStatus.COMPLETED)
            task_result.status = TaskResultStatus.COMPLETED
            task_result.output = output
            return task_result
        case _:
            task_result = TaskResult(status=TaskResultStatus.FAILED)
            task_result.status = TaskResultStatus.FAILED
            task_result.logs = str(response)
            task_result.output = output
            return task_result
