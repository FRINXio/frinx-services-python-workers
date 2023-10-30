from collections import namedtuple
from collections.abc import Generator
from contextlib import contextmanager
from http import HTTPStatus
from operator import itemgetter
from typing import Any
from typing import Literal
from typing import Optional
from typing import Union

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import DictStr
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx_api.resource_manager import ID
from frinx_api.resource_manager import AllocationStrategy
from frinx_api.resource_manager import PoolCapacityPayload
from frinx_api.resource_manager import QueryAllocationStrategiesQuery
from frinx_api.resource_manager import QueryAllocationStrategiesQueryResponse
from frinx_api.resource_manager import QueryPoolCapacityQuery
from frinx_api.resource_manager import QueryPoolCapacityQueryResponse
from frinx_api.resource_manager import QueryResourceTypesQuery
from frinx_api.resource_manager import QueryResourceTypesQueryResponse
from frinx_api.resource_manager import ResourceType
from graphql_pydantic_converter.graphql_types import concatenate_queries
from pydantic import IPvAnyAddress
from python_graphql_client import GraphqlClient
from requests.exceptions import HTTPError

from frinx_worker.resource_manager.env import RESOURCE_MANAGER_URL_BASE
from frinx_worker.resource_manager.errors import InvalidQueryError
from frinx_worker.resource_manager.errors import PoolNotFoundError
from frinx_worker.resource_manager.models_and_enums import ResourceTypeEnum
from frinx_worker.resource_manager.type_aliases import GraphQueryRenderer
from frinx_worker.resource_manager.type_aliases import IPvAddress


@contextmanager
def qraphql_client_manager(endpoint: str, headers: Optional[DictStr] = None,
                           **kwargs: Any) -> Generator[GraphqlClient, None, None]:
    client = GraphqlClient(endpoint=endpoint, headers=headers or {}, **kwargs)
    try:
        yield client
    except HTTPError as error:
        match error.response.status_code:
            case HTTPStatus.UNPROCESSABLE_ENTITY:
                raise InvalidQueryError()
            # TODO: Enhance the error handling.
            case _:
                raise error


def execute_query(graph_query: Union[GraphQueryRenderer, str], variables: Optional[DictAny] = None,
                  operation_name: Optional[str] = None, headers: Optional[DictStr] = None, **kwargs) -> DictAny:
    with qraphql_client_manager(endpoint=RESOURCE_MANAGER_URL_BASE) as client:
        return client.execute(
            query=graph_query if isinstance(graph_query, str) else graph_query.render(),
            variables=variables or {},
            operation_name=operation_name,
            headers=headers or {},
            **kwargs)


def get_resource_type_and_allocation_strategy_id(resource_type: ResourceTypeEnum) -> tuple[ID, ID]:
    query_resource = QueryResourceTypesQuery(byName=resource_type, payload=ResourceType(id=True))
    query_allocation = QueryAllocationStrategiesQuery(byName=resource_type, payload=AllocationStrategy(id=True))
    data = execute_query(concatenate_queries([query_resource, query_allocation]))
    resource, allocation = QueryResourceTypesQueryResponse(**data), QueryAllocationStrategiesQueryResponse(**data)
    return resource.data.query_resource_types[0].id, allocation.data.query_allocation_strategies[0].id


def get_free_and_utilized_capacity_of_pool(pool_id: ID) -> Optional[tuple[int, int]]:
    query = QueryPoolCapacityQuery(
        poolId=pool_id, payload=PoolCapacityPayload(freeCapacity=True, utilizedCapacity=True))
    response_model = QueryPoolCapacityQueryResponse(**execute_query(query))
    try:
        return (int(response_model.data.query_pool_capacity.free_capacity),
                int(response_model.data.query_pool_capacity.utilized_capacity))
    except AttributeError:
        raise PoolNotFoundError(pool_id)


def get_max_prefix_len(ipv: Literal[ResourceTypeEnum.IPV4, ResourceTypeEnum.IPV6]) -> int:
    return 128 if ipv is ResourceTypeEnum.IPV6 else 32


def calculate_available_prefixes(free_capacity: int,
                                 ip_version: Literal[ResourceTypeEnum.IPV4, ResourceTypeEnum.IPV6]) -> DictStr:
    calculated_prefixes = {}
    max_bit_size = get_max_prefix_len(ip_version)

    for prefix in range(1, max_bit_size + 1):
        prefix_capacity = pow(2, max_bit_size - prefix)
        if prefix_capacity <= free_capacity:
            result = free_capacity // prefix_capacity
            calculated_prefixes[f'/{prefix}'] = str(result)

    return calculated_prefixes


IPAddressOwner = namedtuple('IPAddressOwner', ['owner', 'ip_address'])


def sorted_owners_by_ip_addresses(*, reverse: bool = False,
                                  **ip_addresses: Optional[Union[IPvAddress, IPvAnyAddress]]) -> list[IPAddressOwner]:
    return [IPAddressOwner(*_) for _ in sorted(ip_addresses.items(), key=itemgetter(1), reverse=reverse)]


def execute_graph_query__return_task_result(task_output: type[TaskOutput],
                                            graph_query: Union[GraphQueryRenderer, str]) -> TaskResult:
    result = execute_query(graph_query)
    status = TaskResultStatus.FAILED if 'errors' in result else TaskResultStatus.COMPLETED
    return TaskResult(status=status, output=task_output(result=result))
