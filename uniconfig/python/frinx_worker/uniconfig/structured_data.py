from string import Template
from typing import Optional

import requests
from frinx.common.frinx_rest import UNICONFIG_HEADERS
from frinx.common.frinx_rest import UNICONFIG_REQUEST_PARAMS
from frinx.common.frinx_rest import UNICONFIG_URL_BASE
from frinx.common.type_aliases import DictAny
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl

from . import class_to_json
from . import handle_response
from . import uniconfig_zone_to_cookie


class StructuredData(ServiceWorkersImpl):
    class ReadStructuredData(WorkerImpl):
        from frinx_api.uniconfig.rest_api import ReadStructuredData as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_read_structured_device_data'
            description: str = 'Read device configuration or operational data in structured format e.g. openconfig'
            labels: list[str] = ['BASICS', 'UNICONFIG', 'OPENCONFIG']

        class WorkerInput(TaskInput):
            node_id: str
            uri: Optional[str] = None
            topology_id: str = 'uniconfig'
            transaction_id: Optional[str] = None
            uniconfig_server_id: Optional[str] = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            uri = ''
            if worker_input.uri:
                if not worker_input.uri.startswith('/'):
                    uri = f'/{worker_input.uri}'
                else:
                    uri = worker_input.uri

            url = worker_input.uniconfig_url_base + self.UniconfigApi.uri.format(
                topology_id=worker_input.topology_id, node_id=worker_input.node_id, uri=uri
            )

            response = requests.request(
                url=url,
                method=self.UniconfigApi.method,
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            return handle_response(response, self.WorkerOutput)

    class WriteStructuredData(WorkerImpl):
        from frinx_api.uniconfig.rest_api import ReadStructuredData as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_write_structured_device_data'
            description: str = 'Write device configuration data in structured format e.g. openconfig'
            labels: list[str] = ['BASICS', 'UNICONFIG']

        class WorkerInput(TaskInput):
            node_id: str
            uri: Optional[str] = None
            template: DictAny
            method: str = 'PUT'
            params: Optional[DictAny] = {}
            topology_id: str = 'uniconfig'
            transaction_id: Optional[str] = None
            uniconfig_server_id: Optional[str] = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            uri = ''
            if worker_input.uri:
                if not worker_input.uri.startswith('/'):
                    uri = f'/{worker_input.uri}'
                else:
                    uri = worker_input.uri

            url = worker_input.uniconfig_url_base + self.UniconfigApi.uri.format(
                topology_id=worker_input.topology_id, node_id=worker_input.node_id, uri=uri
            )

            if worker_input.params:
                worker_input.template.update(worker_input.params)
                url = Template(url).substitute(worker_input.params)

            response = requests.request(
                url=url,
                method=worker_input.method,
                data=class_to_json(worker_input.template),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            return handle_response(response, self.WorkerOutput)

    class DeleteStructuredData(WorkerImpl):
        from frinx_api.uniconfig.rest_api import DeleteStructuredData as UniconfigApi

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'UNICONFIG_delete_structured_device_data'
            description: str = 'Delete device configuration data in structured format e.g. openconfig'
            labels: list[str] = ['BASICS', 'UNICONFIG']

        class WorkerInput(TaskInput):
            node_id: str
            uri: Optional[str] = None
            template: DictAny
            method: str = 'DELETE'
            params: Optional[DictAny] = {}
            topology_id: str = 'uniconfig'
            transaction_id: Optional[str] = None
            uniconfig_server_id: Optional[str] = None
            uniconfig_url_base: str = UNICONFIG_URL_BASE

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            uri = ''
            if worker_input.uri:
                if not worker_input.uri.startswith('/'):
                    uri = f'/{worker_input.uri}'
                else:
                    uri = worker_input.uri

            url = worker_input.uniconfig_url_base + self.UniconfigApi.uri.format(
                topology_id=worker_input.topology_id, node_id=worker_input.node_id, uri=uri
            )

            if worker_input.params:
                worker_input.template.update(worker_input.params)
                url = Template(url).substitute(worker_input.params)

            response = requests.request(
                url=url,
                method=worker_input.method,
                data=class_to_json(worker_input.template),
                cookies=uniconfig_zone_to_cookie(
                    uniconfig_server_id=worker_input.uniconfig_server_id,
                    transaction_id=worker_input.transaction_id
                ),
                headers=dict(UNICONFIG_HEADERS),
                params=UNICONFIG_REQUEST_PARAMS
            )

            return handle_response(response, self.WorkerOutput)
