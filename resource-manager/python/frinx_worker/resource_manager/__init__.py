import os
from typing import Any
from typing import Literal
from typing import Optional
from typing import Union

from frinx.common.conductor_enums import TimeoutPolicy
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import ListStr
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.task_result import TaskResultStatus
from frinx.common.worker.worker import WorkerImpl
from frinx_api.resource_manager import ID
from frinx_api.resource_manager import AllocationStrategy
from frinx_api.resource_manager import ClaimResourceMutation
from frinx_api.resource_manager import ClaimResourceMutationResponse
from frinx_api.resource_manager import ClaimResourceWithAltIdMutation
from frinx_api.resource_manager import ClaimResourceWithAltIdMutationResponse
from frinx_api.resource_manager import CreateAllocatingPoolInput
from frinx_api.resource_manager import CreateAllocatingPoolMutation
from frinx_api.resource_manager import CreateAllocatingPoolMutationResponse
from frinx_api.resource_manager import CreateAllocatingPoolPayload
from frinx_api.resource_manager import CreateNestedAllocatingPoolInput
from frinx_api.resource_manager import CreateNestedAllocatingPoolMutation
from frinx_api.resource_manager import CreateNestedAllocatingPoolMutationResponse
from frinx_api.resource_manager import CreateNestedAllocatingPoolPayload
from frinx_api.resource_manager import DeleteResourcePoolInput
from frinx_api.resource_manager import DeleteResourcePoolMutation
from frinx_api.resource_manager import DeleteResourcePoolMutationResponse
from frinx_api.resource_manager import DeleteResourcePoolPayload
from frinx_api.resource_manager import FreeResourceMutation
from frinx_api.resource_manager import FreeResourceMutationResponse
from frinx_api.resource_manager import Map
from frinx_api.resource_manager import OutputCursor
from frinx_api.resource_manager import QueryEmptyResourcePoolsQuery
from frinx_api.resource_manager import QueryEmptyResourcePoolsQueryResponse
from frinx_api.resource_manager import QueryRecentlyActiveResourcesQuery
from frinx_api.resource_manager import QueryRecentlyActiveResourcesQueryResponse
from frinx_api.resource_manager import QueryResourcePoolsQuery
from frinx_api.resource_manager import QueryResourcePoolsQueryResponse
from frinx_api.resource_manager import QueryResourcesByAltIdQuery
from frinx_api.resource_manager import QueryResourcesByAltIdQueryResponse
from frinx_api.resource_manager import Resource
from frinx_api.resource_manager import ResourceConnection
from frinx_api.resource_manager import ResourceEdge
from frinx_api.resource_manager import ResourcePool
from frinx_api.resource_manager import ResourcePoolConnection
from frinx_api.resource_manager import ResourcePoolEdge
from frinx_api.resource_manager import SearchPoolsByTagsQuery
from frinx_api.resource_manager import SearchPoolsByTagsQueryResponse
from frinx_api.resource_manager import Tag
from frinx_api.resource_manager import TagAnd
from frinx_api.resource_manager import TagOr
from frinx_api.resource_manager import UpdateResourceAltIdMutation
from pydantic import Field
from pydantic import IPvAnyAddress
from pydantic import NonNegativeInt
from pydantic import model_validator

from frinx_worker.resource_manager.models_and_enums import CustomConfiguredTaskInput
from frinx_worker.resource_manager.models_and_enums import Data
from frinx_worker.resource_manager.models_and_enums import PoolProperties
from frinx_worker.resource_manager.models_and_enums import Report
from frinx_worker.resource_manager.models_and_enums import Result
from frinx_worker.resource_manager.type_aliases import CursorID
from frinx_worker.resource_manager.type_aliases import ISODateTimeString
from frinx_worker.resource_manager.utils import RESOURCE_MANAGER_URL_BASE
from frinx_worker.resource_manager.utils import ResourceTypeEnum
from frinx_worker.resource_manager.utils import calculate_available_prefixes
from frinx_worker.resource_manager.utils import execute_graph_query__return_task_result
from frinx_worker.resource_manager.utils import get_free_and_utilized_capacity_of_pool
from frinx_worker.resource_manager.utils import get_max_prefix_len
from frinx_worker.resource_manager.utils import get_resource_type_and_allocation_strategy_id
from frinx_worker.resource_manager.utils import sorted_owners_by_ip_addresses

SERVICE_NAME = 'RESOURCE_MANAGER'


# QA: allow_population_by_alias, is ok to access to attribute by alias in code ? (mypy, mix-case)
class ResourceManager(ServiceWorkersImpl):

    class ClaimResource(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_claim_resource'
            description: str = 'Allocate new resource in pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_id: ID
            description: Optional[str] = None
            user_input: Optional[Map] = {}
            alternative_id: Optional[Map] = None

        class WorkerOutput(TaskOutput):
            result: Union[ClaimResourceMutationResponse, ClaimResourceWithAltIdMutationResponse]

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            gql_generator = ClaimResourceWithAltIdMutation if worker_input.alternative_id else ClaimResourceMutation
            mutation = gql_generator(
                payload=Resource(id=True, Properties=True, AlternativeId=True),
                **worker_input.model_dump(exclude_none=True, by_alias=True)
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, mutation)

    # QA: Has been removed from resource-manager API since version 2.0.0 ?
    # FIXME: Remove if yes, otherwise ...?
    # class QueryClaimedResources(WorkerImpl):
    #     class ExecutionProperties(TaskExecutionProperties):
    #         exclude_empty_inputs: bool = True
    #         transform_string_to_json_valid: bool = True
    #
    #     class WorkerDefinition(TaskDefinition):
    #         name: str = f'{SERVICE_NAME}_query_claimed_resource'
    #         description: str = 'Search for already allocated resources.'
    #         timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW
    #
    #     class WorkerInput(ResourceManagerTaskInput):
    #         pool_id: ID
    #         alternative_id: Optional[Map] = None
    #
    #     class WorkerOutput(TaskOutput):
    #         result: ...
    #
    #     def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
    #         ...

    class CreatePool(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_create_pool'
            description: str = 'Create resource pool, with specified properties and tags.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_name: str
            resource_type: ResourceTypeEnum
            pool_properties: PoolProperties
            tags: Optional[ListStr] = []

        class WorkerOutput(TaskOutput):
            result: CreateAllocatingPoolMutationResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            resource_type_id, resource_strategy_id = get_resource_type_and_allocation_strategy_id(
                worker_input.resource_type)
            if worker_input.pool_name not in worker_input.tags:
                worker_input.tags.append(worker_input.pool_name)
            # BUG: Rendering issue with address value, missing quotes.
            #   poolProperties: {address: 0.0.0.0, prefix: 24, subnet: False}
            #                             ^
            query = CreateAllocatingPoolMutation(
                input=CreateAllocatingPoolInput(
                    poolName=worker_input.pool_name,
                    tags=worker_input.tags,
                    poolProperties=dict(worker_input.pool_properties),
                    resourceTypeId=resource_type_id,
                    allocationStrategyId=resource_strategy_id,
                    poolDealocationSafetyPeriod=0,
                    # QA: Values should be strings, like this or python types ?
                    poolPropertyTypes={'address': 'string', 'prefix': 'int', 'subnet': 'bool'}
                ),
                payload=CreateAllocatingPoolPayload(pool=ResourcePool(id=True))
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class CreateVLANPool(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_create_vlan_pool'
            description: str = 'Create VLAN pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_name: str
            from_: int = Field(alias='from')
            to: int
            parent_resource_id: Optional[ID] = None
            tags: Optional[ListStr] = []

        class WorkerOutput(TaskOutput):
            result: Union[CreateNestedAllocatingPoolMutationResponse, CreateAllocatingPoolMutationResponse]

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            resource_type_id, resource_strategy_id = get_resource_type_and_allocation_strategy_id(ResourceTypeEnum.VLAN)
            if worker_input.pool_name not in worker_input.tags:
                worker_input.tags.append(worker_input.pool_name)

            if worker_input.parent_resource_id:
                query = CreateNestedAllocatingPoolMutation
                input_ = CreateNestedAllocatingPoolInput(
                    resourceTypeId=resource_type_id,
                    allocationStrategyId=resource_strategy_id,
                    poolDealocationSafetyPeriod=0,
                    poolName=worker_input.pool_name,
                    parentResourceId=worker_input.parent_resource_id,
                    tags=worker_input.tags
                )
                payload = CreateNestedAllocatingPoolPayload(pool=ResourcePool(id=True))
            else:
                query = CreateAllocatingPoolMutation
                input_ = CreateAllocatingPoolInput(
                    allocationStrategyId=resource_strategy_id,
                    poolDealocationSafetyPeriod=0,
                    poolName=worker_input.pool_name,
                    poolProperties={'from': worker_input.from_, 'to': worker_input.to},
                    poolPropertyTypes={'from': int, 'to': int},
                    resourceTypeId=resource_type_id,
                    tags=worker_input.tags
                )
                payload = CreateAllocatingPoolPayload(pool=ResourcePool(id=True))

            return execute_graph_query__return_task_result(self.WorkerOutput, query(input=input_, payload=payload))

    class CreateVLANRangePool(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_create_vlan_range_pool'
            description: str = 'Create VLAN range pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_name: str
            from_: int = Field(alias='from')
            to: int
            tags: Optional[ListStr] = []

        class WorkerOutput(TaskOutput):
            result: CreateAllocatingPoolMutationResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            resource_type_id, resource_strategy_id = get_resource_type_and_allocation_strategy_id(
                ResourceTypeEnum.VLAN_RANGE)
            if worker_input.pool_name not in worker_input.tags:
                worker_input.tags.append(worker_input.pool_name)

            query = CreateAllocatingPoolMutation(
                input=CreateAllocatingPoolInput(
                    allocationStrategyId=resource_strategy_id,
                    poolDealocationSafetyPeriod=0,
                    poolName=worker_input.pool_name,
                    poolProperties={'from': worker_input.from_, 'to': worker_input.to},
                    poolPropertyTypes={'from': int, 'to': int},
                    resourceTypeId=resource_type_id,
                    tags=worker_input.tags
                ),
                payload=CreateAllocatingPoolPayload(pool=ResourcePool(id=True))
            )

            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class CreateUniqueIDPool(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_create_unique_id_pool'
            description: str = 'Create unique ID pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(TaskInput):
            pool_name: str
            id_format: str
            from_: int = Field(alias='from')
            to: int
            tags: Optional[ListStr] = []

        class WorkerOutput(TaskOutput):
            result: CreateAllocatingPoolMutationResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            resource_type_id, resource_strategy_id = get_resource_type_and_allocation_strategy_id(
                ResourceTypeEnum.UNIQUE_ID)
            if worker_input.pool_name not in worker_input.tags:
                worker_input.tags.append(worker_input.pool_name)

            query = CreateAllocatingPoolMutation(
                input=CreateAllocatingPoolInput(
                    allocationStrategyId=resource_strategy_id,
                    poolDealocationSafetyPeriod=0,
                    poolName=worker_input.pool_name,
                    # TODO: create other PoolProperty models for each resource type
                    poolProperties={
                        'idFormat': worker_input.id_format,
                        'from': worker_input.from_,
                        'to': worker_input.to
                    },
                    poolPropertyTypes={'idFormat': 'string', 'from': 'int', 'to': 'int'},
                    resourceTypeId=resource_type_id,
                    tags=worker_input.tags
                ),
                payload=CreateAllocatingPoolPayload(pool=ResourcePool(id=True))
            )

            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class QueryPool(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_query_pool'
            description: str = 'Search for existing pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_names: ListStr
            resource: ResourceTypeEnum

        class WorkerOutput(TaskOutput):
            result: QueryResourcePoolsQueryResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            resource_type_id, _ = get_resource_type_and_allocation_strategy_id(worker_input.resource)
            query = QueryResourcePoolsQuery(
                tags=TagOr(matchesAny=[TagAnd(matchesAll=worker_input.pool_names)]),
                resourceTypeId=resource_type_id,
                payload=ResourcePoolConnection(edges=ResourcePoolEdge(node=ResourcePool(id=True)))
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class QueryUniqueIDPool(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_query_unique_id_pool'
            description: str = 'Search for unique ID pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_names: ListStr

        class WorkerOutput(TaskOutput):
            result: QueryResourcePoolsQueryResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            resource_type_id, _ = get_resource_type_and_allocation_strategy_id(ResourceTypeEnum.UNIQUE_ID)
            query = QueryResourcePoolsQuery(
                tags=TagOr(matchesAny=[TagAnd(matchesAll=worker_input.pool_names)]),
                resourceTypeId=resource_type_id,
                payload=ResourcePoolConnection(edges=ResourcePoolEdge(node=ResourcePool(id=True)))
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class QueryVLANPool(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_query_vlan_pool'
            description: str = 'Search for VLAN pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_names: ListStr

        class WorkerOutput(TaskOutput):
            result: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            # TODO: that's the same code like in QueryUniqueIDPool, abstraction, extraction needed
            resource_type_id, _ = get_resource_type_and_allocation_strategy_id(ResourceTypeEnum.VLAN)
            query = QueryResourcePoolsQuery(
                tags=TagOr(matchesAny=[TagAnd(matchesAll=worker_input.pool_names)]),
                resourceTypeId=resource_type_id,
                payload=ResourcePoolConnection(edges=ResourcePoolEdge(node=ResourcePool(id=True)))
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class QueryPoolByTag(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_query_pool_by_tag'
            description: str = 'Search pools by their tags.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_tag: str

        class WorkerOutput(TaskOutput):
            result: SearchPoolsByTagsQueryResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            query = SearchPoolsByTagsQuery(
                tags=TagOr(
                    matchesAny=[TagAnd(matchesAll=[worker_input.pool_tag])]
                ),
                payload=ResourcePoolConnection(
                    edges=ResourcePoolEdge(
                        node=ResourcePool(
                            Name=True,
                            id=True,
                            AllocationStrategy=AllocationStrategy(
                                Name=True
                            )
                        )
                    )
                )
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class QueryVLANRangePool(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_query_vlan_range_pool'
            description: str = 'Search for VLAN range pools by their names.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_names: ListStr

        class WorkerOutput(TaskOutput):
            result: QueryResourcePoolsQueryResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            resource_type_id, _ = get_resource_type_and_allocation_strategy_id(ResourceTypeEnum.VLAN_RANGE)
            query = QueryResourcePoolsQuery(
                tags=TagOr(
                    matchesAny=[TagAnd(matchesAll=worker_input.pool_names)]
                ),
                resourceTypeId=resource_type_id,
                payload=ResourcePoolConnection(
                    edges=ResourcePoolEdge(
                        node=ResourcePool(
                            id=True
                        )
                    )
                )
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class UpdateAlternativeIDForResource(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_update_alternative_id_for_resource'
            description: str = 'Update alternative ID for resource.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_id: ID
            # TODO: what should be in resource_properties: Map aka ResourceProperties ...
            resource_properties: Map
            alternative_id: ID

        class WorkerOutput(TaskOutput):
            result: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            query = UpdateResourceAltIdMutation(
                payload=Resource(AlternativeId=True),
                **worker_input.model_dump(by_alias=True)
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class ReadXTenant(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = 'Read_x_tenant'
            description: str = 'Input format: X_TENANT_ID'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(TaskInput):
            pass

        class WorkerOutput(TaskOutput):
            result: Result

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            x_tenant = os.getenv('X_TENANT_ID')
            if x_tenant:
                return TaskResult(
                    status=TaskResultStatus.COMPLETED,
                    output=self.WorkerOutput(result={'data': {'X_TENANT_ID': x_tenant}}))
            else:
                return TaskResult(
                    status=TaskResultStatus.COMPLETED,
                    output=self.WorkerOutput(result={'error': 'X_TENANT_ID not found in the environment'}))

    class ReadResourceManagerURLBase(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = 'Read_resource_manager_url_base'
            description: str = 'Input format: RESOURCE_MANAGER_URL_BASE'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(TaskInput):
            pass

        class WorkerOutput(TaskOutput):
            result: Result

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(result={'data': {'RESOURCE_MANAGER_URL_BASE': RESOURCE_MANAGER_URL_BASE}}))

    class AccumulateReport(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_accumulate_report'
            description: str = 'Merge together two reports of available prefixes for address pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True

        class WorkerInput(CustomConfiguredTaskInput):
            first_report: Report
            last_report: Report

        class WorkerOutput(TaskOutput):
            result: Result

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            first_report, last_report = worker_input.first_report.model_dump(), worker_input.last_report.model_dump()
            prefixes_intersection = set(first_report) & set(last_report)
            accumulated_reports = first_report | last_report

            for prefix in prefixes_intersection:
                if prefix in prefixes_intersection:
                    accumulated_reports[prefix] = first_report[prefix] + last_report[prefix]

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(result=Result(data=Data(accumulated_reports))))

    class CalculateAvailablePrefixesForAddressPool(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_calculate_available_prefixes_for_address_pool'
            description: str = 'Calculate all available prefixes for given address pool.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        # QA: What to do if pool is other resource pool let say uniqueId and not ipv pool ?
        class WorkerInput(CustomConfiguredTaskInput):
            pool_id: ID
            resource_type: ResourceTypeEnum

        class WorkerOutput(TaskOutput):
            result: Result

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            free_capacity, _ = get_free_and_utilized_capacity_of_pool(worker_input.pool_id)
            available_prefixes = calculate_available_prefixes(free_capacity, worker_input.resource_type)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(result=Result(data=available_prefixes)))

    class QueryResourceByAlternativeID(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_query_resource_by_alt_id'
            description: str = 'Search for resources by their alternative ID.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerInput(CustomConfiguredTaskInput):
            alternative_id: ID
            pool_id: ID
            first: Optional[NonNegativeInt] = None
            last: Optional[NonNegativeInt] = None
            after: Optional[ISODateTimeString] = None
            before: Optional[ISODateTimeString] = None

        class WorkerOutput(TaskOutput):
            result: QueryResourcesByAltIdQueryResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            query = QueryResourcesByAltIdQuery(
                payload=ResourceConnection(
                    edges=ResourceEdge(
                        cursor=OutputCursor(ID=True),
                        node=Resource(
                            NestedPool=ResourcePool(id=True, Name=True, Tags=Tag(Tag=True)),
                            ParentPool=ResourcePool(id=True, Name=True),
                            Properties=True,
                            AlternativeId=True,
                            id=True
                        )
                    )
                ),
                input={'alternativeId': worker_input.alternative_id},
                **worker_input.model_dump(exclude_none=True, by_alias=True, exclude={'alternative_id'})
            )
            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class DeallocateResource(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_deallocate_resource'
            description: str = 'Free resource for new usage.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class ExecutionProperties(TaskExecutionProperties):
            transform_string_to_json_valid: bool = True

        class WorkerInput(CustomConfiguredTaskInput):
            pool_id: ID
            # QA: How looks schema of user_input AKA. input in FreeResourceMutation, for each pool "type"?
            user_input: Map

        class WorkerOutput(TaskOutput):
            result: FreeResourceMutationResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            mutation = FreeResourceMutation(payload=True, poolId=worker_input.pool_id, input=worker_input.user_input)
            return execute_graph_query__return_task_result(self.WorkerOutput, mutation)

    class DeletePool(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_delete_pool'
            description: str = 'Delete pool by ID.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            pool_id: ID

        class WorkerOutput(TaskOutput):
            result: DeleteResourcePoolMutationResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            mutation = DeleteResourcePoolMutation(
                input=DeleteResourcePoolInput(resourcePoolId=worker_input.pool_id),
                payload=DeleteResourcePoolPayload(resourcePoolId=True))

            return execute_graph_query__return_task_result(self.WorkerOutput, mutation)

    class CalculateHostAndBroadcastAddress(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_calculate_host_and_broadcast_address'
            description: str = 'Calculate host and broadcast for desired size.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerInput(CustomConfiguredTaskInput):
            desired_size: NonNegativeInt
            network_address: Optional[IPvAnyAddress] = None
            provider_address: Optional[IPvAnyAddress] = None
            customer_address: Optional[IPvAnyAddress] = None

        class WorkerOutput(TaskOutput):
            result: Result

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            if worker_input.network_address and worker_input.provider_address and worker_input.customer_address:
                pass
            elif worker_input.provider_address and worker_input.customer_address:
                worker_input.network_address = min(worker_input.provider_address,
                                                   worker_input.customer_address) - 1
            elif worker_input.network_address:
                worker_input.provider_address = worker_input.network_address + 1
                worker_input.customer_address = worker_input.network_address + 2
            else:
                raise ValueError('Need at least networkAddress or providerAddress and customerAddress!')

            broadcast_address = worker_input.network_address + worker_input.desired_size - 1
            owner_of_lower_address, owner_of_higher_address = sorted_owners_by_ip_addresses(
                Provider=worker_input.provider_address, Customer=worker_input.customer_address)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(result=Result(data=Data(
                    network_address=str(worker_input.network_address),
                    broadcast_address=str(broadcast_address),
                    address_capacity_in_subnet=worker_input.desired_size,
                    owner_of_lower_address=owner_of_lower_address.owner,
                    owner_of_higher_address=owner_of_higher_address.owner,
                    lower_address=str(owner_of_lower_address.ip_address)))))

    class CalculateDesiredSizeFromPrefix(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_calculate_desired_size_from_prefix'
            description: str = 'Calculate size for given IPv address from prefix, inclusive subnet.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            prefix: int
            # QA: Could be better to rename it to ipv6: bool, whether is False max_prefix_len is calculated for ipv4?
            resource_type: Literal[ResourceTypeEnum.IPV4, ResourceTypeEnum.IPV6]
            subnet: bool

            @model_validator(mode='after')
            def validate_prefix_size_for_ip_version(self):
                max_prefix_len = get_max_prefix_len(self.resource_type)
                if 1 <= self.prefix <= max_prefix_len:
                    return self
                raise ValueError(f'Prefix must be between 1 and {max_prefix_len} for {self.resource_type}')

        class WorkerOutput(TaskOutput):
            result: Result

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            # FIXME: prefix=32, subnet=True, resource_type='ipv4' -> desired_size=-1
            inverted = get_max_prefix_len(worker_input.resource_type) - worker_input.prefix
            desired_size = pow(2, inverted) - 2 * int(worker_input.subnet)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(result=Result(data=desired_size)))

    class QuerySearchEmptyPools(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_query_search_empty_pools'
            description: str = 'Search for pools which are free for allocating new resources.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            resource_type_id: ID

        class WorkerOutput(TaskOutput):
            result: QueryEmptyResourcePoolsQueryResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            query = QueryEmptyResourcePoolsQuery(
                resourceTypeId=worker_input.resource_type_id,
                payload=ResourcePoolConnection(
                    edges=ResourcePoolEdge(
                        node=ResourcePool(
                            id=True,
                            Name=True,
                            Tags=Tag(Tag=True),
                            AllocationStrategy=AllocationStrategy(
                                Name=True)))))

            return execute_graph_query__return_task_result(self.WorkerOutput, query)

    class QueryRecentlyActiveResources(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = f'{SERVICE_NAME}_query_recently_active_resources'
            description: str = 'Search for recently active resources.'
            timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkerInput(CustomConfiguredTaskInput):
            from_datetime: ISODateTimeString
            to_datetime: Optional[ISODateTimeString] = None
            first: Optional[NonNegativeInt] = None
            last: Optional[NonNegativeInt] = None
            before: Optional[CursorID] = None
            after: Optional[CursorID] = None

        class WorkerOutput(TaskOutput):
            result: QueryRecentlyActiveResourcesQueryResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            # BUG: toDatetime expect optional string, when None is given query holds this param instead of excluding it
            query = QueryRecentlyActiveResourcesQuery(
                payload=ResourceConnection(
                    edges=ResourceEdge(
                        cursor=OutputCursor(ID=True),
                        node=Resource(
                            NestedPool=ResourcePool(id=True, Name=True),
                            ParentPool=ResourcePool(id=True, Name=True),
                            AlternativeId=True,
                            Properties=True,
                            id=True))),
                **worker_input.model_dump(exclude_none=True, by_alias=True))

            # BUG: parsing response throws ValidationError for OutputCursorPayload, should be RootModel is ID
            return execute_graph_query__return_task_result(self.WorkerOutput, query)
