from enum import Enum

import requests
from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.frinx_rest import INVENTORY_HEADERS
from frinx.common.frinx_rest import T0POLOGY_URL_BASE
from frinx.common.type_aliases import DictAny
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from graphql_pydantic_converter.graphql_types import QueryForm
from pydantic import BaseModel
from pydantic import Field


class TopologyWorkerOutput(TaskOutput):
    """Topology-Discovery worker output."""

    query: str = Field(
        description="Constructed GraphQL query.",
    )
    variable: DictAny | None = Field(
        description="Constructed input GraphQL variables.",
        default=None
    )
    response_body: DictAny | None = Field(
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


class TopologyOutput(BaseModel):
    """Parsed response from Topology-Discovery service."""

    status: ResponseStatus = Field(
        description="Response status."
    )
    code: int = Field(
        description="Parsed response code."
    )
    data: DictAny | None = Field(
        default=None,
        description="Structured response data."
    )


def execute_graphql_operation(
        query: str,
        variables: DictAny | None = None,
        topology_url_base: str = T0POLOGY_URL_BASE
) -> TopologyOutput:
    """
    Execute GraphQL query.
    
    :param query: GraphQL query
    :param variables: GraphQL variables in dictionary format
    :param topology_url_base: Topology-Discovery service URL base
    :return: TopologyOutput object
    """ ""
    response = requests.post(
        topology_url_base,
        json={
            "query": query,
            "variables": variables
        },
        headers=INVENTORY_HEADERS
    )
    data = response.json()
    status_code = response.status_code

    if data.get("errors") is not None:
        return TopologyOutput(data=data["errors"], status=ResponseStatus.ERRORS, code=status_code)

    if data.get("data") is not None:
        return TopologyOutput(data=data["data"], status=ResponseStatus.DATA, code=status_code)

    return TopologyOutput(status=ResponseStatus.FAILED, code=status_code)


def response_handler(query: QueryForm, response: TopologyOutput) -> TaskResult:
    """
    Handle response from Topology-Discovery service.

    :param query: GraphQL query information
    :param response: parsed topology-discovery response
    :return: built TaskResult object
    """
    output = TopologyWorkerOutput(
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
