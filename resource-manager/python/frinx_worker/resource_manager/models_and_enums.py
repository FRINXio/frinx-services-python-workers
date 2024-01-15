from enum import Enum
from typing import Any
from typing import Optional

from frinx.common.worker.task_def import TaskInput
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import IPvAnyAddress
from pydantic import NonNegativeInt
from pydantic import RootModel
from pydantic.alias_generators import to_camel

from frinx_worker.resource_manager.type_aliases import IPv4PrefixTypeAlias

Data = RootModel[Optional[Any]]


class Result(BaseModel):
    data: Optional[Any]


class CustomConfiguredTaskInput(TaskInput):
    model_config = ConfigDict(alias_generator=to_camel)


# FIXME: refactor, each pool has own properties, this one is probably for ipv pools
class PoolProperties(BaseModel):
    address: IPvAnyAddress
    prefix: int
    subnet: Optional[bool] = False


class IPv4NetMaskSize(RootModel):
    root: NonNegativeInt = Field(le=2**32)


class Report(RootModel):
    root: dict[IPv4PrefixTypeAlias, IPv4NetMaskSize]


class ResourceTypeEnum(Enum):
    RANDOM_SIGNED_INT32 = 'random_signed_int32'
    ROUTE_DISTINGUISHER = 'route_distinguisher'
    IPV6_PREFIX = 'ipv6_prefix'
    IPV4_PREFIX = 'ipv4_prefix'
    VLAN_RANGE = 'vlan_range'
    UNIQUE_ID = 'unique_id'
    IPV6 = 'ipv6'
    IPV4 = 'ipv4'
    VLAN = 'vlan'
