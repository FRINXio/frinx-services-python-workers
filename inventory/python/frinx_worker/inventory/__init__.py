import copy
from enum import Enum
from typing import Any
from typing import Optional

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import DictStr
from frinx.common.type_aliases import ListAny
from frinx.common.type_aliases import ListStr
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx_api.inventory import AddDeviceInput
from frinx_api.inventory import AddDeviceMutation
from frinx_api.inventory import AddDevicePayload
from frinx_api.inventory import CreateLabelInput
from frinx_api.inventory import CreateLabelMutation
from frinx_api.inventory import CreateLabelPayload
from frinx_api.inventory import Device
from frinx_api.inventory import DeviceConnection
from frinx_api.inventory import DeviceEdge
from frinx_api.inventory import DeviceServiceState
from frinx_api.inventory import DeviceSize
from frinx_api.inventory import DevicesQuery
from frinx_api.inventory import FilterDevicesInput
from frinx_api.inventory import InstallDeviceMutation
from frinx_api.inventory import InstallDevicePayload
from frinx_api.inventory import Label
from frinx_api.inventory import LabelConnection
from frinx_api.inventory import LabelEdge
from frinx_api.inventory import LabelsQuery
from frinx_api.inventory import PageInfo
from frinx_api.inventory import UninstallDeviceMutation
from frinx_api.inventory import UninstallDevicePayload
from frinx_api.inventory import Zone
from frinx_api.inventory import ZoneEdge
from frinx_api.inventory import ZonesConnection
from frinx_api.inventory import ZonesQuery

from .utils import CoursorGroup
from .utils import CoursorGroups
from .utils import InventoryOutput
from .utils import execute_inventory_query


class PaginationCursorType(str, Enum):
    AFTER = 'after'
    BEFORE = 'before'
    NONE = None


class InventoryService(ServiceWorkersImpl):

    class InventoryGetDevicesInfo(WorkerImpl):

        DEVICES: DeviceConnection = DeviceConnection(
            pageInfo=PageInfo(
                hasNextPage=True,
                hasPreviousPage=True,
                startCursor=True,
                endCursor=True
            ),
            edges=DeviceEdge(
                node=Device(
                    id=True,
                    name=True,
                    port=False,
                    serviceState=True,
                    isInstalled=True,
                    mountParameters=True,
                    zone=Zone(
                        name=True,
                        id=True
                    )
                )
            )
        )

        DevicesQuery(
            payload=DEVICES,
            filter=FilterDevicesInput(
                deviceName='name',
                labels=['LABELS']
            ),
            first=10,
            last=10,
            after='after',
            before='before',
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_get_device_info'
            description: str = 'Get a list of pages cursors from device inventory'
            labels: ListStr = ['BASIC', 'INVENTORY']

        class WorkerInput(TaskInput):
            device_name: Optional[str]
            labels: Optional[ListStr]
            size: Optional[int]
            cursor: Optional[str]
            type: Optional[PaginationCursorType]

        class WorkerOutput(TaskOutput):
            query: str
            response_code: int
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            query = DevicesQuery(
                payload=self.DEVICES,
                filter=FilterDevicesInput(
                    deviceName=worker_input.device_name or None,
                    labels=worker_input.labels or None
                )
            )

            match worker_input.type:
                case PaginationCursorType.AFTER:
                    query.first = worker_input.size
                    query.after = worker_input.cursor
                case PaginationCursorType.BEFORE:
                    query.last = worker_input.size
                    query.before = worker_input.cursor

            query_render = query.render()
            response = execute_inventory_query(query=query_render, variables=None)

            return response_handler(query_render, response)

    class InventoryInstallDeviceById(WorkerImpl):

        mutation: InstallDeviceMutation = InstallDeviceMutation(
            payload=InstallDevicePayload(
                device=Device(
                    name=True,
                    id=True,
                    isInstalled=True,
                    zone=Zone(
                        name=True,
                        id=True
                    )
                )
            ),
            id='id'
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_install_device_by_id'
            description: str = 'Install device by device ID'
            labels: ListStr = ['BASIC', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_id: str

        class WorkerOutput(TaskOutput):
            query: str
            variables: Optional[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            self.mutation.id = worker_input.device_id
            query_render = self.mutation.render()
            response = execute_inventory_query(query=query_render, variables=None)
            return response_handler(query_render, response)

    class InventoryUninstallDeviceById(WorkerImpl):

        mutation: UninstallDeviceMutation = UninstallDeviceMutation(
            payload=UninstallDevicePayload(
                device=Device(
                    name=True,
                    id=True,
                    isInstalled=True,
                    zone=Zone(
                        name=True,
                        id=True
                    )
                )
            ),
            id='id'
        )

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_uninstall_device_by_id'
            description: str = 'Uninstall device by device ID'
            labels: ListStr = ['BASIC', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_id: str

        class WorkerOutput(TaskOutput):
            query: str
            variables: Optional[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.mutation.id = worker_input.device_id
            query_render = self.mutation.render()
            response = execute_inventory_query(query=query_render, variables=None)
            return response_handler(query_render, response)

    class InventoryInstallDeviceByName(WorkerImpl):

        DEVICES: DeviceConnection = DeviceConnection(
            pageInfo=PageInfo(
                hasNextPage=True,
                hasPreviousPage=True,
                startCursor=True,
                endCursor=True
            ),
            edges=DeviceEdge(
                node=Device(
                    id=True,
                    name=True,
                    port=False,
                    serviceState=True,
                    isInstalled=True,
                    mountParameters=True,
                    zone=Zone(
                        name=True,
                        id=True
                    )
                )
            )
        )

        DevicesQuery(
            payload=DEVICES,
            filter=FilterDevicesInput(
                deviceName='name',
                labels=['LABELS']
            ),
            first=10,
            last=10,
            after='after',
            before='before',
        )

        mutation: InstallDeviceMutation = InstallDeviceMutation(
            payload=InstallDevicePayload(
                device=Device(
                    name=True,
                    id=True,
                    isInstalled=True,
                    zone=Zone(
                        name=True,
                        id=True
                    )
                )
            ),
            id=''
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):

            name: str = 'INVENTORY_install_device_by_name'
            description: str = 'Install device by device name'
            labels: ListAny = ['BASIC', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_name: str

        class WorkerOutput(TaskOutput):
            url: str
            response_code: int
            response_body: Any

        @classmethod
        def _get_device_id(cls, device_name: str) -> str | None:
            query = DevicesQuery(
                payload=cls.DEVICES,
                filter=FilterDevicesInput(
                    deviceName=device_name
                )
            )

            response = execute_inventory_query(query=query.render(), variables=None)

            for node in response.data['devices']['edges']:
                if node['node']['name'] == device_name:
                    return node['node']['id'] or None

            raise Exception('Device ' + device_name + ' missing in inventory')

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.mutation.id = self._get_device_id(worker_input.device_name)
            query_render = self.mutation.render()
            response = execute_inventory_query(query=query_render, variables=None)
            return response_handler(query_render, response)

    class InventoryUninstallDeviceByName(WorkerImpl):

        DEVICES: DeviceConnection = DeviceConnection(
            pageInfo=PageInfo(
                hasNextPage=True,
                hasPreviousPage=True,
                startCursor=True,
                endCursor=True
            ),
            edges=DeviceEdge(
                node=Device(
                    id=True,
                    name=True,
                    port=False,
                    serviceState=True,
                    isInstalled=True,
                    mountParameters=True,
                    zone=Zone(
                        name=True,
                        id=True
                    )
                )
            )
        )

        DevicesQuery(
            payload=DEVICES,
            filter=FilterDevicesInput(
                deviceName='name',
                labels=['LABELS']
            ),
            first=10,
            last=10,
            after='after',
            before='before',
        )

        mutation: UninstallDeviceMutation = UninstallDeviceMutation(
            payload=UninstallDevicePayload(
                device=Device(
                    name=True,
                    id=True,
                    isInstalled=True,
                    zone=Zone(
                        name=True,
                        id=True
                    )
                )
            ),
            id=''
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_uninstall_device_by_name'
            description: str = 'Uninstall device by device name'
            labels: ListStr = ['BASIC', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_name: str

        class WorkerOutput(TaskOutput):
            url: str
            response_code: int
            response_body: Any

        @classmethod
        def _get_device_id(cls, device_name: str) -> str | None:
            query = DevicesQuery(
                payload=cls.DEVICES,
                filter=FilterDevicesInput(
                    deviceName=device_name
                )
            )

            response = execute_inventory_query(query=query.render(), variables=None)

            for node in response.data['devices']['edges']:
                if node['node']['name'] == device_name:
                    return node['node']['id'] or None

            raise Exception('Device ' + device_name + ' missing in inventory')

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.mutation.id = self._get_device_id(worker_input.device_name)
            query_render = self.mutation.render()
            response = execute_inventory_query(query=query_render, variables=None)
            return response_handler(query_render, response)

    class InventoryGetLabels(WorkerImpl):

        LABELS: LabelConnection = LabelConnection(
            pageInfo=PageInfo(
                hasNextPage=True,
                hasPreviousPage=True,
                startCursor=True,
                endCursor=True
            ),
            edges=LabelEdge(
                node=Label(
                    id=True,
                    name=True,
                )
            )
        )

        LabelsQuery(
            payload=LABELS,
            first=10,
            last=10,
            after='after',
            before='before',
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_get_labels'
            description: str = 'Get device labels'
            labels: ListStr = ['BASICS', 'MAIN', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            size: Optional[int]
            cursor: Optional[str]
            type: Optional[PaginationCursorType]

        class WorkerOutput(TaskOutput):
            url: str
            response_code: int
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            query = LabelsQuery(
                payload=self.LABELS,
            )

            match worker_input.type:
                case PaginationCursorType.AFTER:
                    query.first = worker_input.size
                    query.after = worker_input.cursor
                case PaginationCursorType.BEFORE:
                    query.last = worker_input.size
                    query.before = worker_input.cursor

            query_render = query.render()
            response = execute_inventory_query(query=query_render, variables=None)
            return response_handler(query_render, response)

    class InventoryGetLabelsId(WorkerImpl):

        LABELS: LabelConnection = LabelConnection(
            edges=LabelEdge(
                node=Label(
                    id=True,
                    name=True,
                )
            )
        )

        query = LabelsQuery(
            payload=LABELS,
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_get_labels_id'
            description: str = """
            Get id for selected inventory labels.
            If no label is inserted, return empty dict.
            If one of inserted labels missing, return FAILED status.
            """
            labels: ListStr = ['BASICS', 'MAIN', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            labels: Optional[ListStr]

        class WorkerOutput(TaskOutput):
            labels_id: DictStr

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            response = execute_inventory_query(query=self.query.render(), variables=None)
            labels_id: DictStr = {}

            if worker_input.labels:
                for label in response.data['labels']['edges']:
                    if label['node']['name'] in worker_input.labels:
                        labels_id[label['node']['name']] = label['node']['id']

                if len(labels_id.keys()) != len(worker_input.labels):
                    raise Exception('One or more selected labels not exist in device inventory')

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    labels_id=labels_id,
                )
            )

    class InventoryCreateLabel(WorkerImpl):

        mutation: CreateLabelMutation = CreateLabelMutation(
            payload=CreateLabelPayload(
                label=Label(
                    name=True,
                    id=True
                )
            ),
            input=CreateLabelInput(
                name='name'
            )
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_create_label'
            description: str = 'Create device labels'
            labels: ListStr = ['BASICS', 'MAIN', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            label: str

        class WorkerOutput(TaskOutput):
            url: str
            response_code: int
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.mutation.input.name = worker_input.label
            query_render = self.mutation.render()
            response = execute_inventory_query(query=query_render, variables=None)
            return response_handler(query_render, response)

    class InventoryAddDevice(WorkerImpl):

        ADD_DEVICE: AddDevicePayload = AddDevicePayload(
            device=Device(
                name=True,
                id=True,
                isInstalled=True
            )
        )

        mutation: AddDeviceMutation = AddDeviceMutation(
            payload=ADD_DEVICE,
            input=AddDeviceInput(
                name='name',
                zoneId='zoneId',
                mountParameters='{}',
            )
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_add_device'
            description: str = 'Add device to inventory database'
            labels: ListStr = ['BASICS', 'MAIN', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_name: str
            zone_id: str
            mount_parameters: DictAny
            service_state: DeviceServiceState
            device_size: DeviceSize
            vendor: Optional[str]
            model: Optional[str]
            label_ids: Optional[ListStr]
            blueprint_id: Optional[str]
            address: Optional[str]
            port: Optional[int]
            username: Optional[str]
            password: Optional[str]
            version: Optional[str]
            device_type: Optional[str]

        class WorkerOutput(TaskOutput):
            url: str
            response_code: int
            response_body: Any

        @staticmethod
        def _get_zone_id(zone_name: str) -> str | None:
            query = ZonesQuery(
                payload=ZonesConnection(
                    edges=ZoneEdge(
                        node=Zone(
                            name=True,
                            id=True
                        )
                    )
                )
            )

            response = execute_inventory_query(query=query.render(), variables=None)

            for node in response.data['zones']['edges']:
                if node['node']['name'] == zone_name:
                    return node['node']['id'] or None

            raise Exception('Device ' + zone_name + ' missing in inventory')

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            self.mutation.input.name = worker_input.device_name
            self.mutation.input.zone_id = worker_input.zone_id
            self.mutation.input.service_state = worker_input.service_state,
            self.mutation.input.device_size = worker_input.device_size,
            self.mutation.input.mount_parameters = str(worker_input.mount_parameters).replace("'", '\\"')

            if worker_input.label_ids:
                self.mutation.input.label_ids = worker_input.label_ids,

            if worker_input.vendor:
                self.mutation.input.vendor = worker_input.vendor,

            if worker_input.model:
                self.mutation.input.model = worker_input.model,

            if worker_input.device_type:
                self.mutation.input.device_type = worker_input.device_type,

            if worker_input.version:
                self.mutation.input.version = worker_input.version,

            if worker_input.address:
                self.mutation.input.address = worker_input.address,

            if worker_input.port:
                self.mutation.input.port = worker_input.port,

            if worker_input.username:
                self.mutation.input.username = worker_input.username,

            if worker_input.password:
                self.mutation.input.password = worker_input.password,

            query_render = self.mutation.render()

            response = execute_inventory_query(query=query_render, variables=None)
            return response_handler(query_render, response)

    class InventoryGetPagesCursors(WorkerImpl):

        DEVICES: DeviceConnection = DeviceConnection(
            pageInfo=PageInfo(
                hasNextPage=True,
                hasPreviousPage=True,
                startCursor=True,
                endCursor=True
            ),
            edges=DeviceEdge(
                node=Device(
                    id=True,
                    name=True,
                    isInstalled=False
                )
            )
        )

        DevicesQuery(
            payload=DEVICES,
            filter=FilterDevicesInput(
                deviceName='name',
                labels=['LABELS']
            ),
            first=10,
            after='after',
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_get_pages_cursors'
            description: str = 'Get a list of pages cursors from device inventory'
            labels: ListStr = ['BASIC', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            labels: Optional[DictStr]
            cursor_step: Optional[int] = 10
            cursors_per_group: Optional[int] = 20

        class WorkerOutput(TaskOutput):
            cursors_per_group: int
            cursor_step: int
            number_of_groups: int
            cursors_groups: CoursorGroups


        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            labels = None
            if worker_input.labels:
                labels = [label_id for label_id in worker_input.labels.keys()]

            query = DevicesQuery(
                payload=self.DEVICES,
                filter=FilterDevicesInput(
                    labels=labels
                ),
            )
            response = execute_inventory_query(query=query.render(), variables=None)

            datastructure = {}
            dynamic_fork_list = []
            dynamic_fork_dict = {}
            fork_dict_position = 0
            loop_dict_position = 0
            for_loop_id = 0

            match response.status:
                case 'data':
                    for device in range(len(response.data['devices']['edges'])):
                        dynamic_fork_list.append(
                            {
                                response.data['devices']['edges'][device]['node']['name']:
                                response.data['devices']['edges'][device]['node']['id']
                            }
                        )

                        dynamic_fork_dict[f'fork_{fork_dict_position}'] = dynamic_fork_list
                        datastructure[str(f'loop_{loop_dict_position}')] = dynamic_fork_dict

                        if for_loop_id == worker_input.cursors_per_group:
                            dynamic_fork_list = []
                            fork_dict_position += 1
                            for_loop_id = 0
                        if fork_dict_position == worker_input.cursor_step:
                            loop_dict_position += 1
                            fork_dict_position = 0
                            for_loop_id = 0
                            dynamic_fork_dict = {}
                        for_loop_id += 1
                case _:
                    raise Exception(response.data)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    cursors_groups=datastructure,
                    number_of_groups=len(datastructure),
                    cursors_per_group=worker_input.cursors_per_group,
                    cursor_step=worker_input.cursor_step
                )
            )

    class InventoryGetPagesCursorsForkTasks(WorkerImpl):

        TASK_BODY_TEMPLATE: DictAny = {
            'name': 'sub_task',
            'taskReferenceName': '',
            'type': 'SUB_WORKFLOW',
            'subWorkflowParam': {
                'name': '',
                'version': 1
            }
        }

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_get_pages_cursors_fork_tasks'
            description: str = 'Get all pages cursors as dynamic fork tasks'
            labels: ListStr = ['BASIC', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            task: str
            cursors_groups: CoursorGroup

        class WorkerOutput(TaskOutput):
            dynamic_tasks_input: DictAny
            dynamic_tasks: list[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            dynamic_tasks = []
            dynamic_tasks_i = {}

            for fork_id, devices_list in worker_input.cursors_groups.items():
                task_body = copy.deepcopy(self.TASK_BODY_TEMPLATE)
                task_body['taskReferenceName'] = str(fork_id)
                task_body['subWorkflowParam']['name'] = worker_input.task
                dynamic_tasks.append(task_body)
                dynamic_tasks_i[str(fork_id)] = dict(devices=devices_list)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    dynamic_tasks_input=dynamic_tasks_i,
                    dynamic_tasks=dynamic_tasks,
                )
            )

    class InventoryInstallInBatch(WorkerImpl):

        mutation: InstallDeviceMutation = InstallDeviceMutation(
            payload=InstallDevicePayload(
                device=Device(
                    name=True,
                    id=True,
                    isInstalled=False
                )
            ),
            id=''
        )

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_install_in_batch'
            description: str = 'Install devices in batch'
            labels: ListStr = ['BASIC', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600
            limit_to_thread_count = 5

        class WorkerInput(TaskInput):
            devices: list[DictStr]

        class WorkerOutput(TaskOutput):
            response_body: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            device_status = {}
            for device in worker_input.devices:
                per_device_params = dict({})
                for device_name, device_id in device.items():
                    per_device_params.update({'device_id': device_id})
                    per_device_params.update({'device_name': device_name})

                self.mutation.id = per_device_params['device_id']
                response = execute_inventory_query(query=self.mutation.render(), variables=None)

                match response.status:
                    case 'errors':
                        if 'already been installed' not in response.data['message']:
                            per_device_params.update({'status': 'failed'})
                        elif 'already been installed' in response.data['message']:
                            per_device_params.update({'status': 'was installed before'})
                    case 'data':
                        per_device_params.update({'status': 'success'})

                device_status.update({self.mutation.id: per_device_params})

            return TaskResult(
                status=TaskResultStatus.FAILED,
                output=self.WorkerOutput(
                    response_body=device_status,
                )
            )

    class InventoryUninstallInBatch(WorkerImpl):

        mutation: UninstallDeviceMutation = UninstallDeviceMutation(
            payload=UninstallDevicePayload(
                device=Device(
                    name=True,
                    id=True,
                    isInstalled=False
                )
            ),
            id=''
        )

        class WorkerDefinition(TaskDefinition):
            name: str = 'INVENTORY_uninstall_in_batch'
            description: str = 'Uninstall devices in batch'
            labels: ListStr = ['BASIC', 'INVENTORY']
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            devices: list[DictStr]

        class WorkerOutput(TaskOutput):
            response_body: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            device_status = {}
            for device in worker_input.devices:
                per_device_params = dict({})
                for device_name, device_id in device.items():
                    per_device_params.update({'device_id': device_id})
                    per_device_params.update({'device_name': device_name})

                self.mutation.id = per_device_params['device_id']
                response = execute_inventory_query(query=self.mutation.render(), variables=None)

                match response.status:
                    case 'errors':
                        per_device_params.update({'status': 'failed'})
                    case 'data':
                        per_device_params.update({'status': 'success'})

                device_status.update({self.mutation.id: per_device_params})

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    response_body=device_status,
                )
            )


def response_handler(query: str, response: InventoryOutput) -> TaskResult:
    match response.status:
        case 'data':
            task_result = TaskResult(status=TaskResultStatus.COMPLETED)
            task_result.status = TaskResultStatus.COMPLETED
            task_result.output = dict(
                response_code=response.code,
                response_body=response.data,
                query=query
            )
            return task_result
        case _:
            task_result = TaskResult(status=TaskResultStatus.FAILED)
            task_result.status = TaskResultStatus.FAILED
            task_result.logs = str(response)
            task_result.output = dict(
                response_code=response.code,
                response_body=response.data,
                query=query
            )
            return task_result
