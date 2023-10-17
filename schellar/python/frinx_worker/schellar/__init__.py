from enum import Enum
from json import dumps as json_dumps
from typing import Optional

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.frinx_rest import SCHELLAR_HEADERS
from frinx.common.frinx_rest import SCHELLAR_URL_BASE
from frinx.common.graphql.client import GraphqlClient
from frinx.common.type_aliases import DictAny
from frinx.common.util import snake_to_camel_case
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx_api.schellar import CreateScheduleInput
from frinx_api.schellar import CreateScheduleMutation
from frinx_api.schellar import DeleteScheduleMutation
from frinx_api.schellar import PageInfo
from frinx_api.schellar import Schedule
from frinx_api.schellar import ScheduleConnection
from frinx_api.schellar import ScheduleEdge
from frinx_api.schellar import ScheduleQuery
from frinx_api.schellar import SchedulesFilterInput
from frinx_api.schellar import SchedulesQuery
from frinx_api.schellar import UpdateScheduleInput
from frinx_api.schellar import UpdateScheduleMutation

client = GraphqlClient(endpoint=SCHELLAR_URL_BASE, headers=SCHELLAR_HEADERS)


class PaginationCursorType(str, Enum):
    AFTER = 'after'
    BEFORE = 'before'
    NONE = None


class Schellar(ServiceWorkersImpl):
    class GetSchedules(WorkerImpl):

        SCHEDULES: ScheduleConnection = ScheduleConnection(
            pageInfo=PageInfo(
                hasNextPage=True,
                hasPreviousPage=True,
                startCursor=True,
                endCursor=True
            ),
            edges=ScheduleEdge(
                node=Schedule(
                    name=True,
                    cronString=True,
                    enabled=True
                ),
            )
        )

        SchedulesQuery(
            payload=SCHEDULES,
            first=10,
            last=10,
            after='after',
            before='before',
        )

        SchedulesFilterInput(
            workflowName='',
            workflowVersion=''
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'SCHELLAR_get_schedules'
            description: str = 'Get schedules from schellar'

        class WorkerInput(TaskInput):
            workflow_name: Optional[str] = None
            workflow_version: Optional[str] = None
            size: Optional[int] = None
            cursor: Optional[str] = None
            type: Optional[PaginationCursorType] = None

            model_config = TaskInput().model_config
            model_config['alias_generator'] = snake_to_camel_case
            model_config['populate_by_name'] = True

        class WorkerOutput(TaskOutput):
            query: str
            variable: Optional[DictAny] = None
            response: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            schedules = SchedulesQuery(
                payload=self.SCHEDULES
            )

            if worker_input.workflow_name and worker_input.workflow_version:
                schedules.filter = SchedulesFilterInput(
                    workflowName=worker_input.workflow_name,
                    workflowVersion=worker_input.workflow_version
                )
            elif worker_input.workflow_name or worker_input.workflow_version:
                raise Exception('Missing combination of inputs')

            match worker_input.type:
                case PaginationCursorType.AFTER:
                    schedules.first = worker_input.size
                    schedules.after = worker_input.cursor
                case PaginationCursorType.BEFORE:
                    schedules.last = worker_input.size
                    schedules.before = worker_input.cursor

            query = schedules.render()
            response = client.execute(query=query.query, variables=query.variable)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    response=response,
                    query=query.query,
                    variable=query.variable
                )
            )

    class GetSchedule(WorkerImpl):

        SCHEDULE: Schedule = Schedule(
            name=True,
            enabled=True,
            workflowName=True,
            workflowVersion=True,
            cronString=True
        )

        ScheduleQuery(
            payload=SCHEDULE,
            name='name',
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'SCHELLAR_get_schedule'
            description: str = 'Get schedule by name from schellar'

        class WorkerInput(TaskInput):
            name: str

        class WorkerOutput(TaskOutput):
            query: str
            variable: Optional[DictAny] = None
            response: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            schedule = ScheduleQuery(
                payload=self.SCHEDULE,
                name=worker_input.name
            )

            query = schedule.render()
            response = client.execute(query=query.query, variables=query.variable)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    response=response,
                    query=query.query,
                    variable=query.variable
                )
            )

    class DeleteSchedule(WorkerImpl):

        DeleteScheduleMutation(
            payload=True,
            name='name',
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'SCHELLAR_delete_schedule'
            description: str = 'Delete schedule from schellar'

        class WorkerInput(TaskInput):
            name: str

        class WorkerOutput(TaskOutput):
            query: str
            variable: Optional[DictAny] = None
            response: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            delete_schedule = DeleteScheduleMutation(
                payload=True,
                name=worker_input.name
            )

            mutation = delete_schedule.render()
            response = client.execute(query=mutation.query, variables=mutation.variable)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    response=response,
                    query=mutation.query,
                    variable=mutation.variable
                )
            )

    class CreateSchedule(WorkerImpl):

        SCHEDULE: Schedule = Schedule(
            name=True,
            enabled=True,
            workflowName=True,
            workflowVersion=True,
            cronString=True
        )

        CreateScheduleMutation(
            payload=SCHEDULE,
            input=CreateScheduleInput(
                name='name',
                workflowName='workflowName',
                workflowVersion='workflowVersion',
                cronString='* * * * *',
                enabled=True,
                parallelRuns=False,
                workflowContext='workflowContext',
                fromDate='fromDate',
                toDate='toDate'
            )
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'SCHELLAR_create_schedule'
            description: str = 'Create schellar schedule'

        class WorkerInput(TaskInput):
            name: str
            workflow_name: str
            workflow_version: str
            cron_string: str
            enabled: Optional[bool] = None
            parallel_runs: Optional[bool] = None
            workflow_context: Optional[DictAny] = None
            from_date: Optional[str] = None
            to_date: Optional[str] = None

            model_config = TaskInput().model_config
            model_config['alias_generator'] = snake_to_camel_case
            model_config['populate_by_name'] = True

        class WorkerOutput(TaskOutput):
            query: str
            variable: Optional[DictAny] = None
            response: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            create_schedule = CreateScheduleMutation(
                payload=self.SCHEDULE,
                input=CreateScheduleInput(
                    **worker_input.model_dump(
                        by_alias=True,
                        exclude_none=True,
                        exclude={'workflow_context'}
                    )
                )
            )

            if worker_input.workflow_context:
                create_schedule.input.workflow_context = json_dumps(
                    worker_input.workflow_context
                ).replace('"', '\\"')

            mutation = create_schedule.render(form='extracted')
            response = client.execute(query=mutation.query, variables=mutation.variable)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    response=response,
                    query=mutation.query,
                    variable=mutation.variable
                )
            )

    class UpdateSchedule(WorkerImpl):

        SCHEDULE: Schedule = Schedule(
            name=True,
            enabled=True,
            workflowName=True,
            workflowVersion=True,
            cronString=True
        )

        UpdateScheduleMutation(
            payload=SCHEDULE,
            name='name',
            input=UpdateScheduleInput(
                workflowName='workflowName',
                workflowVersion='workflowVersion',
                cronString='* * * * *',
                enabled=True,
                parallelRuns=False,
                workflowContext='workflowContext',
                fromDate='fromDate',
                toDate='toDate'
            )
        )

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = 'SCHELLAR_update_schedule'
            description: str = 'Update schellar schedule by name'

        class WorkerInput(TaskInput):
            name: str
            workflow_name: Optional[str] = None
            workflow_version: Optional[str] = None
            cron_string: Optional[str] = None
            enabled: Optional[bool] = None
            parallel_runs: Optional[bool] = None
            workflow_context: Optional[DictAny] = None
            from_date: Optional[str] = None
            to_date: Optional[str] = None

            model_config = TaskInput().model_config
            model_config['alias_generator'] = snake_to_camel_case
            model_config['populate_by_name'] = True

        class WorkerOutput(TaskOutput):
            query: str
            variable: Optional[DictAny] = None
            response: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            update_schedule = UpdateScheduleMutation(
                name=worker_input.name,
                payload=self.SCHEDULE,
                input=UpdateScheduleInput(
                    **worker_input.model_dump(
                        by_alias=True,
                        exclude_none=True,
                        exclude={'name', 'workflow_context'}
                    )
                )
            )

            if worker_input.workflow_context:
                update_schedule.input.workflow_context = json_dumps(
                    worker_input.workflow_context
                ).replace('"', '\\"')

            mutation = update_schedule.render()
            response = client.execute(query=mutation.query, variables=mutation.variable)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    response=response,
                    query=mutation.query,
                    variable=mutation.variable
                )
            )
