from ipaddress import IPv4Address
from ipaddress import IPv6Address

import pydantic
import requests
from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.frinx_rest import UNICONFIG_URL_BASE
from frinx.common.type_aliases import ListStr
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.service import WorkerImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx_api.uniconfig import OperationsDiscoverPostResponse
from frinx_api.uniconfig.device.discovery.discover import Address
from frinx_api.uniconfig.device.discovery.discover import Input
from frinx_api.uniconfig.device.discovery.discover import TcpPortItem
from frinx_api.uniconfig.device.discovery.discover import UdpPortItem
from frinx_api.uniconfig.rest_api import Discover
from pydantic import Field
from pydantic import IPvAnyAddress
from pydantic import IPvAnyNetwork

from . import class_to_json
from .util import get_list_of_ip_addresses
from .util import parse_ranges


class DeviceDiscoveryWorkers(ServiceWorkersImpl):
    class DeviceDiscoveryWorker(WorkerImpl):

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = False
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):

            name: str = 'UNICONFIG_device_discovery'
            description: str = 'Verification of reachable devices in a network.'
            labels: ListStr = ['BASICS', 'UNICONFIG']

        class WorkerInput(TaskInput):
            ip: list[Address]
            tcp_port: list[TcpPortItem] | None = None
            udp_port: list[UdpPortItem] | None = Field(None, max_length=500)

            @pydantic.field_validator('ip', mode='before')
            @classmethod
            def validate_ip(cls, ip: str) -> list[Address]:

                ip_list = get_list_of_ip_addresses(ip)
                if len(ip_list) == 1:
                    if '/' in ip_list[0]:
                        address = Address(network=str(IPvAnyNetwork(ip_list[0])))
                    else:
                        address = Address(ip_address=str(IPvAnyAddress(ip_list[0])))
                else:
                    try:
                        address = Address(
                            start_ipv4_address=str(IPv4Address(ip_list[0])),
                            end_ipv4_address=str(IPv4Address(ip_list[1]))
                        )
                    except ValueError:
                        address = Address(
                            start_ipv6_address=str(IPv6Address(ip_list[0])),
                            end_ipv6_address=str(IPv6Address(ip_list[1]))
                        )

                return [address]

            @pydantic.field_validator('tcp_port', mode='before')
            @classmethod
            def validate_tcp(cls, tcp_port: str) -> list[TcpPortItem]:
                return [TcpPortItem(port=p) for p in parse_ranges(tcp_port.split(','))]

            @pydantic.field_validator('udp_port', mode='before')
            @classmethod
            def validate_udp(cls, udp_port: str) -> list[UdpPortItem]:
                return [UdpPortItem(port=p) for p in parse_ranges(udp_port.split(','))]

        class WorkerOutput(TaskOutput):
            output: OperationsDiscoverPostResponse

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            if Discover.request is None:
                raise Exception(f'Failed to create request {Discover.request}')

            if Discover.response is None:
                raise Exception(f'Failed to create request {Discover.response}')

            template = Input(
                address=worker_input.ip,
                tcp_port=worker_input.tcp_port or None,
                udp_port=worker_input.udp_port or None,
            )

            response = requests.request(
                url=UNICONFIG_URL_BASE + Discover.uri,
                method=Discover.method,
                data=class_to_json(
                    Discover.request(
                        input=template
                    ),
                ),
            )

            if response.ok:
                status = TaskResultStatus.COMPLETED

                return TaskResult(
                    status=status,
                    output=self.WorkerOutput(
                        output=Discover.response(
                            output=response.json()['output']
                        )
                    )
                )

            status = TaskResultStatus.FAILED
            return TaskResult(
                status=status,
                output=self.WorkerOutput(
                    output=response.json()
                    )
                )
