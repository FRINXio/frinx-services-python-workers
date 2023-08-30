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
            workflow_name: Optional[str]
            workflow_version: Optional[str]
            size: Optional[int]
            cursor: Optional[str]
            type: Optional[PaginationCursorType]

            class Config:
                alias_generator = snake_to_camel_case
                allow_population_by_field_name = True

        class WorkerOutput(TaskOutput):
            query: str
            variables: Optional[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            query = SchedulesQuery(
                payload=self.SCHEDULES
            )

            if worker_input.workflow_name and worker_input.workflow_version:
                query.filter = SchedulesFilterInput(
                    workflowName=worker_input.workflow_name,
                    workflowVersion=worker_input.workflow_version
                )
            elif worker_input.workflow_name or worker_input.workflow_version:
                raise Exception('Missing combination of inputs')

            match worker_input.type:
                case PaginationCursorType.AFTER:
                    query.first = worker_input.size
                    query.after = worker_input.cursor
                case PaginationCursorType.BEFORE:
                    query.last = worker_input.size
                    query.before = worker_input.cursor

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(query=query.render(), variables=None)
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
            variables: Optional[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            query = ScheduleQuery(
                payload=self.SCHEDULE,
                name=worker_input.name
            )

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(query=query.render(), variables=None)
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
            variables: Optional[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            mutation = DeleteScheduleMutation(
                payload=True,
                name=worker_input.name
            )

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(query=mutation.render(), variables=None)
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
            enabled: Optional[bool]
            parallel_runs: Optional[bool]
            workflow_context: Optional[DictAny]
            from_date: Optional[str]
            to_date: Optional[str]

            class Config(TaskInput.Config):
                alias_generator = snake_to_camel_case
                allow_population_by_field_name = True

        class WorkerOutput(TaskOutput):
            query: str
            variables: Optional[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:

            mutation = CreateScheduleMutation(
                payload=self.SCHEDULE,
                input=CreateScheduleInput(
                    **worker_input.dict(
                        by_alias=True,
                        exclude_none=True,
                        exclude={'workflow_context'}
                    )
                )
            )

            if worker_input.workflow_context:
                mutation.input.workflow_context = json_dumps(worker_input.workflow_context).replace('"', '\\"')

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(query=mutation.render(), variables=None)
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
            workflow_name: Optional[str]
            workflow_version: Optional[str]
            cron_string: Optional[str]
            enabled: Optional[bool]
            parallel_runs: Optional[bool]
            workflow_context: Optional[DictAny]
            from_date: Optional[str]
            to_date: Optional[str]

            class Config:
                alias_generator = snake_to_camel_case
                allow_population_by_field_name = True

        class WorkerOutput(TaskOutput):
            query: str
            variables: Optional[DictAny]

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            mutation = UpdateScheduleMutation(
                name=worker_input.name,
                payload=self.SCHEDULE,
                input=UpdateScheduleInput(
                    **worker_input.dict(
                        by_alias=True,
                        exclude_none=True,
                        exclude={'name', 'workflow_context'}
                    )
                )
            )

            if worker_input.workflow_context:
                mutation.input.workflow_context = json_dumps(worker_input.workflow_context).replace('"', '\\"')

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(query=mutation.render(), variables=None)
            )

    class ExecuteSchellarQuery(WorkerImpl):
        class WorkerDefinition(TaskDefinition):
            name: str = 'Execute_schellar_query'
            description: str = 'Execute schellar query'

        class WorkerInput(TaskInput):
            query: str
            variables: Optional[DictAny]

        class WorkerOutput(TaskOutput):
            output: DictAny

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            response = client.execute(query=worker_input.query, variables=worker_input.variables)
            return TaskResult(status=TaskResultStatus.COMPLETED, output=self.WorkerOutput(output=response))
