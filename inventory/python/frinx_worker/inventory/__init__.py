import copy
import json
from enum import Enum
from typing import Any

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
from frinx_api.inventory import DeleteDeviceMutation
from frinx_api.inventory import DeleteDevicePayload
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
from frinx_api.inventory import UpdateDeviceInput
from frinx_api.inventory import UpdateDeviceMutation
from frinx_api.inventory import UpdateDevicePayload
from frinx_api.inventory import Zone
from frinx_api.inventory import ZoneEdge
from frinx_api.inventory import ZonesConnection
from frinx_api.inventory import ZonesQuery
from graphql_pydantic_converter.graphql_types import QueryForm

from .utils import CursorGroup
from .utils import CursorGroups
from .utils import InventoryOutput
from .utils import execute_inventory_query


class PaginationCursorType(str, Enum):
    AFTER = "after"
    BEFORE = "before"
    NONE = None


class DeviceInput(TaskInput):
    mount_parameters: DictAny
    service_state: DeviceServiceState
    device_size: DeviceSize
    vendor: str | None = None
    model: str | None = None
    label_ids: ListStr | None = None
    blueprint_id: str | None = None
    address: str | None = None
    port: int | None = None
    username: str | None = None
    password: str | None = None
    version: str | None = None
    device_type: str | None = None


class InventoryService(ServiceWorkersImpl):
    class InventoryGetDevicesInfo(WorkerImpl):
        DEVICES: DeviceConnection = DeviceConnection(
            pageInfo=PageInfo(hasNextPage=True, hasPreviousPage=True, startCursor=True, endCursor=True),
            edges=DeviceEdge(
                node=Device(
                    id=True,
                    name=True,
                    port=False,
                    serviceState=True,
                    isInstalled=True,
                    mountParameters=True,
                    zone=Zone(name=True, id=True),
                )
            ),
        )

        DevicesQuery(
            payload=DEVICES,
            filter=FilterDevicesInput(deviceName="name", labels=["LABELS"]),
            first=10,
            last=10,
            after="after",
            before="before",
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_get_device_info"
            description: str = "Get a list of pages cursors from device inventory"
            labels: ListStr = ["BASIC", "INVENTORY"]

        class WorkerInput(TaskInput):
            device_name: str | None = None
            labels: ListStr | None = None
            size: int | None = None
            cursor: str | None = None
            type: PaginationCursorType | None = None

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            devices = DevicesQuery(
                payload=self.DEVICES,
                filter=FilterDevicesInput(
                    deviceName=worker_input.device_name or None, labels=worker_input.labels or None
                ),
            )

            match worker_input.type:
                case PaginationCursorType.AFTER:
                    devices.first = worker_input.size
                    devices.after = worker_input.cursor
                case PaginationCursorType.BEFORE:
                    devices.last = worker_input.size
                    devices.before = worker_input.cursor

            query = devices.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)

            return response_handler(query=query, response=response)

    class InventoryInstallDeviceById(WorkerImpl):
        install_device: InstallDeviceMutation = InstallDeviceMutation(
            payload=InstallDevicePayload(
                device=Device(name=True, id=True, isInstalled=True, zone=Zone(name=True, id=True))
            ),
            id="id",
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_install_device_by_id"
            description: str = "Install device by device ID"
            labels: ListStr = ["BASIC", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_id: str

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response_code: int
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.install_device.id = worker_input.device_id
            query = self.install_device.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

    class InventoryUninstallDeviceById(WorkerImpl):
        uninstall_device: UninstallDeviceMutation = UninstallDeviceMutation(
            payload=UninstallDevicePayload(
                device=Device(name=True, id=True, isInstalled=True, zone=Zone(name=True, id=True))
            ),
            id="id",
        )

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_uninstall_device_by_id"
            description: str = "Uninstall device by device ID"
            labels: ListStr = ["BASIC", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_id: str

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response_code: int
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.uninstall_device.id = worker_input.device_id
            query = self.uninstall_device.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

    class InventoryInstallDeviceByName(WorkerImpl):
        DEVICES: DeviceConnection = DeviceConnection(
            pageInfo=PageInfo(hasNextPage=True, hasPreviousPage=True, startCursor=True, endCursor=True),
            edges=DeviceEdge(
                node=Device(
                    id=True,
                    name=True,
                    port=False,
                    serviceState=True,
                    isInstalled=True,
                    mountParameters=True,
                    zone=Zone(name=True, id=True),
                )
            ),
        )

        DevicesQuery(
            payload=DEVICES,
            filter=FilterDevicesInput(deviceName="name", labels=["LABELS"]),
            first=10,
            last=10,
            after="after",
            before="before",
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_install_device_by_name"
            description: str = "Install device by device name"
            labels: ListAny = ["BASIC", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_name: str

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response_code: int
            response_body: Any

        @classmethod
        def _get_device_id(cls, device_name: str) -> str:
            query = DevicesQuery(payload=cls.DEVICES, filter=FilterDevicesInput(deviceName=device_name)).render()

            response = execute_inventory_query(query=query.query, variables=query.variable)

            for node in response.data["devices"]["edges"]:
                if node["node"]["name"] == device_name:
                    return str(node["node"]["id"])

            raise Exception("Device " + device_name + " missing in inventory")

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            install_device: InstallDeviceMutation = InstallDeviceMutation(
                payload=InstallDevicePayload(
                    device=Device(name=True, id=True, isInstalled=True, zone=Zone(name=True, id=True))
                ),
                id=self._get_device_id(worker_input.device_name)
            )
            
            query = install_device.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

    class InventoryUninstallDeviceByName(WorkerImpl):
        DEVICES: DeviceConnection = DeviceConnection(
            pageInfo=PageInfo(hasNextPage=True, hasPreviousPage=True, startCursor=True, endCursor=True),
            edges=DeviceEdge(
                node=Device(
                    id=True,
                    name=True,
                    port=False,
                    serviceState=True,
                    isInstalled=True,
                    mountParameters=True,
                    zone=Zone(name=True, id=True),
                )
            ),
        )

        DevicesQuery(
            payload=DEVICES,
            filter=FilterDevicesInput(deviceName="name", labels=["LABELS"]),
            first=10,
            last=10,
            after="after",
            before="before",
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_uninstall_device_by_name"
            description: str = "Uninstall device by device name"
            labels: ListStr = ["BASIC", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_name: str

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response_body: Any

        def _get_device_id(self, device_name: str) -> str:
            query = DevicesQuery(payload=self.DEVICES, filter=FilterDevicesInput(deviceName=device_name)).render()

            response = execute_inventory_query(query=query.query, variables=query.variable)

            for node in response.data["devices"]["edges"]:
                if node["node"]["name"] == device_name:
                    return str(node["node"]["id"])

            raise Exception("Device " + device_name + " missing in inventory")

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            uninstall_device: UninstallDeviceMutation = UninstallDeviceMutation(
                payload=UninstallDevicePayload(
                    device=Device(name=True, id=True, isInstalled=True, zone=Zone(name=True, id=True))
                ),
                id=self._get_device_id(worker_input.device_name)
            )
            
            query = uninstall_device.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

    class InventoryGetLabels(WorkerImpl):
        LABELS: LabelConnection = LabelConnection(
            pageInfo=PageInfo(hasNextPage=True, hasPreviousPage=True, startCursor=True, endCursor=True),
            edges=LabelEdge(
                node=Label(
                    id=True,
                    name=True,
                )
            ),
        )

        LabelsQuery(
            payload=LABELS,
            first=10,
            last=10,
            after="after",
            before="before",
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_get_labels"
            description: str = "Get device labels"
            labels: ListStr = ["BASICS", "MAIN", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            size: int | None = None
            cursor: str | None = None
            type: PaginationCursorType | None = None

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response_code: int
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            labels = LabelsQuery(
                payload=self.LABELS,
            )

            match worker_input.type:
                case PaginationCursorType.AFTER:
                    labels.first = worker_input.size
                    labels.after = worker_input.cursor
                case PaginationCursorType.BEFORE:
                    labels.last = worker_input.size
                    labels.before = worker_input.cursor

            query = labels.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

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
        ).render()

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_get_labels_id"
            description: str = """
            Get id for selected inventory labels.
            If no label is inserted, return empty dict.
            If one of inserted labels missing, return FAILED status.
            """
            labels: ListStr = ["BASICS", "MAIN", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            labels: ListStr | None = None

        class WorkerOutput(TaskOutput):
            labels_id: DictStr
            query: str
            variable: DictAny | None = None

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            response = execute_inventory_query(query=self.query.query, variables=self.query.variable)
            labels_id: DictStr = {}

            if worker_input.labels:
                for label in response.data["labels"]["edges"]:
                    if label["node"]["name"] in worker_input.labels:
                        labels_id[label["node"]["name"]] = label["node"]["id"]

                if len(labels_id.keys()) != len(worker_input.labels):
                    raise Exception("One or more selected labels not exist in device inventory")

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(labels_id=labels_id, query=self.query.query, variable=self.query.variable),
            )

    class InventoryCreateLabel(WorkerImpl):
        create_label: CreateLabelMutation = CreateLabelMutation(
            payload=CreateLabelPayload(label=Label(name=True, id=True)), input=CreateLabelInput(name="name")
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_create_label"
            description: str = "Create device labels"
            labels: ListStr = ["BASICS", "MAIN", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            label: str

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.create_label.input.name = worker_input.label
            query = self.create_label.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

    class InventoryAddDevice(WorkerImpl):
        ADD_DEVICE: AddDevicePayload = AddDevicePayload(device=Device(name=True, id=True, isInstalled=True))

        add_device: AddDeviceMutation = AddDeviceMutation(
            payload=ADD_DEVICE,
            input=AddDeviceInput(
                name="name",
                zoneId="zoneId",
                mountParameters="{}",
            ),
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_add_device"
            description: str = "Add device to inventory database"
            labels: ListStr = ["BASICS", "MAIN", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(DeviceInput):
            device_name: str
            zone_id: str

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.add_device.input.name = worker_input.device_name
            self.add_device.input.zone_id = InventoryService._get_zone_id(worker_input.zone_id)
            InventoryService._set_device_input_fields(self.add_device.input, worker_input)

            query = self.add_device.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

    class InventoryGetPagesCursors(WorkerImpl):
        DEVICES: DeviceConnection = DeviceConnection(
            pageInfo=PageInfo(hasNextPage=True, hasPreviousPage=True, startCursor=True, endCursor=True),
            edges=DeviceEdge(node=Device(id=True, name=True, isInstalled=False)),
        )

        DevicesQuery(
            payload=DEVICES,
            filter=FilterDevicesInput(deviceName="name", labels=["LABELS"]),
            first=10,
            after="after",
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_get_pages_cursors"
            description: str = "Get a list of pages cursors from device inventory"
            labels: ListStr = ["BASIC", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            labels: DictStr | None = None
            cursor_step: int | None = 10
            cursors_per_group: int | None = 20

        class WorkerOutput(TaskOutput):
            cursors_per_group: int | None
            cursor_step: int | None
            number_of_groups: int
            cursors_groups: CursorGroups

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            labels = None
            if worker_input.labels:
                labels = [label_id for label_id in worker_input.labels.keys()]

            query = DevicesQuery(
                payload=self.DEVICES,
                filter=FilterDevicesInput(labels=labels),
            ).render()

            response = execute_inventory_query(query=query.query, variables=query.variable)

            datastructure = {}
            dynamic_fork_list = []
            dynamic_fork_dict = {}
            fork_dict_position = 0
            loop_dict_position = 0
            for_loop_id = 0

            match response.status:
                case "data":
                    for device in range(len(response.data["devices"]["edges"])):
                        for_loop_id += 1

                        dynamic_fork_list.append(
                            {
                                response.data["devices"]["edges"][device]["node"]["name"]: response.data["devices"][
                                    "edges"
                                ][device]["node"]["id"]
                            }
                        )

                        dynamic_fork_dict[f"fork_{fork_dict_position}"] = dynamic_fork_list
                        datastructure[str(f"loop_{loop_dict_position}")] = dynamic_fork_dict

                        if for_loop_id == worker_input.cursors_per_group:
                            dynamic_fork_list = []
                            fork_dict_position += 1
                            for_loop_id = 0
                        if fork_dict_position == worker_input.cursor_step:
                            loop_dict_position += 1
                            fork_dict_position = 0
                            for_loop_id = 0
                            dynamic_fork_dict = {}
                case _:
                    raise Exception(response.data)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    cursors_groups=datastructure,
                    number_of_groups=len(datastructure),
                    cursors_per_group=worker_input.cursors_per_group,
                    cursor_step=worker_input.cursor_step,
                ),
            )

    class InventoryGetPagesCursorsForkTasks(WorkerImpl):
        TASK_BODY_TEMPLATE: DictAny = {
            "name": "sub_task",
            "taskReferenceName": "",
            "type": "SUB_WORKFLOW",
            "subWorkflowParam": {"name": "", "version": 1},
        }

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_get_pages_cursors_fork_tasks"
            description: str = "Get all pages cursors as dynamic fork tasks"
            labels: ListStr = ["BASIC", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            task: str
            cursors_groups: CursorGroup

        class WorkerOutput(TaskOutput):
            dynamic_tasks_input: DictAny
            dynamic_tasks: list[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            dynamic_tasks = []
            dynamic_tasks_i = {}

            for fork_id, devices_list in worker_input.cursors_groups.items():
                task_body = copy.deepcopy(self.TASK_BODY_TEMPLATE)
                task_body["taskReferenceName"] = str(fork_id)
                task_body["subWorkflowParam"]["name"] = worker_input.task
                dynamic_tasks.append(task_body)
                dynamic_tasks_i[str(fork_id)] = dict(devices=devices_list)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    dynamic_tasks_input=dynamic_tasks_i,
                    dynamic_tasks=dynamic_tasks,
                ),
            )

    class InventoryInstallInBatch(WorkerImpl):
        install_device: InstallDeviceMutation = InstallDeviceMutation(
            payload=InstallDevicePayload(device=Device(name=True, id=True, isInstalled=False)), id=""
        )

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_install_in_batch"
            description: str = "Install devices in batch"
            labels: ListStr = ["BASIC", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600
            limit_to_thread_count: int = 5

        class WorkerInput(TaskInput):
            devices: list[DictStr]

        class WorkerOutput(TaskOutput):
            response_body: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            device_status = {}
            for device in worker_input.devices:
                per_device_params = dict({})
                for device_name, device_id in device.items():
                    per_device_params.update({"device_id": device_id})
                    per_device_params.update({"device_name": device_name})

                self.install_device.id = per_device_params["device_id"]
                query = self.install_device.render()
                response = execute_inventory_query(query=query.query, variables=query.variable)

                match response.status:
                    case "errors":
                        if "already been installed" not in response.data["message"]:
                            per_device_params.update({"status": "failed"})
                        elif "already been installed" in response.data["message"]:
                            per_device_params.update({"status": "was installed before"})
                    case "data":
                        per_device_params.update({"status": "success"})

                device_status.update({self.install_device.id: per_device_params})

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    response_body=device_status,
                ),
            )

    class InventoryUninstallInBatch(WorkerImpl):
        uninstall_device: UninstallDeviceMutation = UninstallDeviceMutation(
            payload=UninstallDevicePayload(device=Device(name=True, id=True, isInstalled=False)), id=""
        )

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_uninstall_in_batch"
            description: str = "Uninstall devices in batch"
            labels: ListStr = ["BASIC", "INVENTORY"]
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
                    per_device_params.update({"device_id": device_id})
                    per_device_params.update({"device_name": device_name})

                self.uninstall_device.id = per_device_params["device_id"]
                query = self.uninstall_device.render()
                response = execute_inventory_query(query=query.query, variables=query.variable)

                match response.status:
                    case "errors":
                        per_device_params.update({"status": "failed"})
                    case "data":
                        per_device_params.update({"status": "success"})

                device_status.update({self.uninstall_device.id: per_device_params})

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    response_body=device_status,
                ),
            )

    class InventorySubWorkflowForkFormat(WorkerImpl):
        TASK_BODY_TEMPLATE: DictAny = {
            "name": "sub_task",
            "taskReferenceName": "",
            "type": "SUB_WORKFLOW",
            "subWorkflowParam": {"name": "", "version": 1},
        }

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_sub_workflow_format"
            description: str = "Get all pages cursors as dynamic fork tasks with device name for uniconfig workers"
            labels: ListStr = ["BASIC", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            cursors_groups: CursorGroup
            task: str
            task_input: DictAny | None = None
            device_identifies: str = "device_id"

        class WorkerOutput(TaskOutput):
            dynamic_tasks_input: DictAny
            dynamic_tasks: list[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            dynamic_tasks = []
            dynamic_tasks_i = {}
            devices = []

            for fork_id, devices_list in worker_input.cursors_groups.items():
                for device_info in devices_list:
                    for device_name in device_info:
                        devices.append(device_name)

            for device in devices:
                task_body = copy.deepcopy(self.TASK_BODY_TEMPLATE)
                task_body["taskReferenceName"] = str(device)
                task_body["subWorkflowParam"]["name"] = worker_input.task
                dynamic_tasks.append(task_body)

                task_input: DictAny = {worker_input.device_identifies: device}

                if worker_input.task_input:
                    task_input.update(**worker_input.task_input)

                dynamic_tasks_i.update({str(device): copy.deepcopy(task_input)})

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    dynamic_tasks_input=dynamic_tasks_i,
                    dynamic_tasks=dynamic_tasks,
                ),
            )

    class InventoryDeleteDevice(WorkerImpl):
        DELETE_DEVICE: DeleteDevicePayload = DeleteDevicePayload(device=Device(name=True, id=True))

        delete_device: DeleteDeviceMutation = DeleteDeviceMutation(
            payload=DELETE_DEVICE,
            id="deviceId"
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_delete_device"
            description: str = "Delete device from inventory database"
            labels: ListStr = ["BASICS", "MAIN", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(TaskInput):
            device_id: str
            """
            Identifier of the removed device.
            """

        class WorkerOutput(TaskOutput):
            query: str
            """
            Request GraphQL query.
            """
            variable: DictAny | None = None
            """
            Request GraphQL variables.
            """
            response_body: Any
            """
            Response containing the name and identifier of the removed device.
            """
            response_code: int
            """
            Status of the operation.
            """

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.delete_device.id = worker_input.device_id
            query = self.delete_device.render()

            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

    class InventoryUpdateDevice(WorkerImpl):
        UPDATE_DEVICE: UpdateDevicePayload = UpdateDevicePayload(device=Device(name=True, id=True, isInstalled=True))

        update_device: UpdateDeviceMutation = UpdateDeviceMutation(
            payload=UPDATE_DEVICE,
            id="deviceId",
            input=UpdateDeviceInput(
                mountParameters="{}",
            ),
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "INVENTORY_update_device"
            description: str = "Update device in inventory database"
            labels: ListStr = ["BASICS", "MAIN", "INVENTORY"]
            timeout_seconds: int = 3600
            response_timeout_seconds: int = 3600

        class WorkerInput(DeviceInput):
            device_id: str
            location_id: str | None = None

        class WorkerOutput(TaskOutput):
            query: str
            variable: DictAny | None = None
            response_code: int
            response_body: Any

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            self.update_device.id = worker_input.device_id
            InventoryService._set_device_input_fields(self.update_device.input, worker_input)
            if worker_input.location_id:
                self.update_device.input.location_id = worker_input.location_id

            query = self.update_device.render()
            response = execute_inventory_query(query=query.query, variables=query.variable)
            return response_handler(query, response)

    @staticmethod
    def _set_device_input_fields(
            query_input: UpdateDeviceInput | AddDeviceInput,
            device_input: DeviceInput
    ) -> None:
        query_input.service_state = device_input.service_state
        query_input.device_size = device_input.device_size
        query_input.mount_parameters = json.dumps(device_input.mount_parameters)
        if device_input.label_ids:
            query_input.label_ids = device_input.label_ids
        if device_input.vendor:
            query_input.vendor = device_input.vendor
        if device_input.model:
            query_input.model = device_input.model
        if device_input.device_type:
            query_input.device_type = device_input.device_type
        if device_input.version:
            query_input.version = device_input.version
        if device_input.address:
            query_input.address = device_input.address
        if device_input.port:
            query_input.port = device_input.port
        if device_input.username:
            query_input.username = device_input.username
        if device_input.password:
            query_input.password = device_input.password

    @staticmethod
    def _get_zone_id(zone_name: str) -> str:
        query = ZonesQuery(payload=ZonesConnection(edges=ZoneEdge(node=Zone(name=True, id=True)))).render()

        response = execute_inventory_query(query=query.query, variables=query.variable)

        for node in response.data["zones"]["edges"]:
            if node["node"]["name"] == zone_name:
                return str(node["node"]["id"])

        raise Exception("Device " + zone_name + " missing in inventory")


def response_handler(query: QueryForm, response: InventoryOutput) -> TaskResult:
    match response.status:
        case "data":
            task_result = TaskResult(status=TaskResultStatus.COMPLETED)
            task_result.status = TaskResultStatus.COMPLETED
            task_result.output = dict(
                response_code=response.code, response_body=response.data, query=query.query, variable=query.variable
            )
            return task_result
        case _:
            task_result = TaskResult(status=TaskResultStatus.FAILED)
            task_result.status = TaskResultStatus.FAILED
            task_result.logs = str(response)
            task_result.output = dict(
                response_code=response.code, response_body=response.data, query=query.query, variable=query.variable
            )
            return task_result
