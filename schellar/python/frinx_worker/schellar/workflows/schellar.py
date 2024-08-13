from frinx.common.workflow.service import ServiceWorkflowsImpl
from frinx.common.workflow.task import SimpleTask
from frinx.common.workflow.task import SimpleTaskInputParameters
from frinx.common.workflow.workflow import FrontendWFInputFieldType
from frinx.common.workflow.workflow import WorkflowImpl
from frinx.common.workflow.workflow import WorkflowInputField

from .. import PaginationCursorType
from .. import Schellar


class SchellarWorkflows(ServiceWorkflowsImpl):
    class ScheduleWorkflow(WorkflowImpl):
        name: str = "Schedule_workflow"
        version: int = 1
        description: str = "Schedule workflow as a cron job"
        labels: list[str] = ["SCHELLAR"]

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            name: WorkflowInputField = WorkflowInputField(
                name="name",
                frontend_default_value="",
                description="Unique schedule name",
                type=FrontendWFInputFieldType.STRING,
            )

            workflow_name: WorkflowInputField = WorkflowInputField(
                name="workflow_name",
                frontend_default_value="",
                description="Existing workflow name",
                type=FrontendWFInputFieldType.STRING,
            )

            workflow_version: WorkflowInputField = WorkflowInputField(
                name="workflow_version",
                frontend_default_value="1",
                description="Existing workflow version",
                type=FrontendWFInputFieldType.STRING,
            )

            cron_string: WorkflowInputField = WorkflowInputField(
                name="cron_string",
                frontend_default_value="* * * * *",
                description="Cron Expression",
                type=FrontendWFInputFieldType.STRING,
            )

            enabled: WorkflowInputField = WorkflowInputField(
                name="enabled",
                frontend_default_value=True,
                description="Enable workflow execution",
                type=FrontendWFInputFieldType.TOGGLE,
            )

            parallel_runs: WorkflowInputField = WorkflowInputField(
                name="parallel_runs",
                frontend_default_value=False,
                description="Enable parallel executions of scheduled workflow",
                type=FrontendWFInputFieldType.TOGGLE,
            )

            workflow_context: WorkflowInputField = WorkflowInputField(
                name="workflow_context",
                frontend_default_value="",
                description="Workflow input parameters in json format",
                type=FrontendWFInputFieldType.JSON,
            )

            from_date: WorkflowInputField = WorkflowInputField(
                name="from_date",
                frontend_default_value="",
                description="Example of format: 2023-05-17T10:15:00Z ",
                type=FrontendWFInputFieldType.STRING,
            )

            to_date: WorkflowInputField = WorkflowInputField(
                name="to_date",
                frontend_default_value="",
                description="Example of format: 2023-05-17T10:15:00Z ",
                type=FrontendWFInputFieldType.STRING,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            self.tasks.append(
                SimpleTask(
                    name=Schellar.CreateSchedule,
                    task_reference_name="schedule_workflow",
                    input_parameters=SimpleTaskInputParameters(
                        root=dict(
                            name=workflow_inputs.name.wf_input,
                            workflow_name=workflow_inputs.workflow_name.wf_input,
                            workflow_version=workflow_inputs.workflow_version.wf_input,
                            cron_string=workflow_inputs.cron_string.wf_input,
                            enabled=workflow_inputs.enabled.wf_input,
                            parallel_runs=workflow_inputs.parallel_runs.wf_input,
                            workflow_context=workflow_inputs.workflow_context.wf_input,
                            from_date=workflow_inputs.from_date.wf_input,
                            to_date=workflow_inputs.to_date.wf_input,
                        )
                    ),
                )
            )

            self.output_parameters = self.WorkflowOutput(response=self.tasks[0].output_ref("response"))

    class UpdateScheduledWorkflow(WorkflowImpl):
        name: str = "Update_scheduled_workflow"
        version: int = 1
        description: str = "Update scheduled workflow"
        labels: list[str] = ["SCHELLAR"]

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            name: WorkflowInputField = WorkflowInputField(
                name="name",
                frontend_default_value="",
                description="Schedule name",
                type=FrontendWFInputFieldType.STRING,
            )

            workflow_name: WorkflowInputField = WorkflowInputField(
                name="workflow_name",
                frontend_default_value="",
                description="Existing workflow name",
                type=FrontendWFInputFieldType.STRING,
            )

            workflow_version: WorkflowInputField = WorkflowInputField(
                name="workflow_version",
                frontend_default_value="1",
                description="Existing workflow version",
                type=FrontendWFInputFieldType.STRING,
            )

            cron_string: WorkflowInputField = WorkflowInputField(
                name="cron_string",
                frontend_default_value="* * * * *",
                description="Cron Expression",
                type=FrontendWFInputFieldType.STRING,
            )

            enabled: WorkflowInputField = WorkflowInputField(
                name="enabled",
                frontend_default_value=None,
                description="Enable workflow execution",
                type=FrontendWFInputFieldType.TOGGLE,
            )

            parallel_runs: WorkflowInputField = WorkflowInputField(
                name="parallel_runs",
                frontend_default_value=None,
                description="Enable parallel executions of scheduled workflow",
                type=FrontendWFInputFieldType.TOGGLE,
            )

            workflow_context: WorkflowInputField = WorkflowInputField(
                name="workflow_context",
                frontend_default_value="",
                description="Workflow input parameters in json format",
                type=FrontendWFInputFieldType.JSON,
            )

            from_date: WorkflowInputField = WorkflowInputField(
                name="from_date",
                frontend_default_value="",
                description="Example of format: 2023-05-17T10:15:00Z ",
                type=FrontendWFInputFieldType.STRING,
            )

            to_date: WorkflowInputField = WorkflowInputField(
                name="to_date",
                frontend_default_value="",
                description="Example of format: 2023-05-17T10:15:00Z ",
                type=FrontendWFInputFieldType.STRING,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            self.tasks.append(
                SimpleTask(
                    name=Schellar.UpdateSchedule,
                    task_reference_name="update_scheduled_workflow",
                    input_parameters=SimpleTaskInputParameters(
                        root=dict(
                            name=workflow_inputs.name.wf_input,
                            workflow_name=workflow_inputs.workflow_name.wf_input,
                            workflow_version=workflow_inputs.workflow_version.wf_input,
                            cron_string=workflow_inputs.cron_string.wf_input,
                            enabled=workflow_inputs.enabled.wf_input,
                            parallel_runs=workflow_inputs.parallel_runs.wf_input,
                            workflow_context=workflow_inputs.workflow_context.wf_input,
                            from_date=workflow_inputs.from_date.wf_input,
                            to_date=workflow_inputs.to_date.wf_input,
                        )
                    ),
                )
            )

            self.output_parameters = self.WorkflowOutput(response=self.tasks[0].output_ref("response"))

    class DeleteScheduledWorkflow(WorkflowImpl):
        name: str = "Delete_scheduled_workflow"
        version: int = 1
        description: str = "Delete scheduled workflow"
        labels: list[str] = ["SCHELLAR"]

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            name: WorkflowInputField = WorkflowInputField(
                name="name",
                frontend_default_value="",
                description="Schedule name",
                type=FrontendWFInputFieldType.STRING,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            self.tasks.append(
                SimpleTask(
                    name=Schellar.DeleteSchedule,
                    task_reference_name="delete_scheduled_workflow",
                    input_parameters=SimpleTaskInputParameters(root=dict(name=workflow_inputs.name.wf_input)),
                )
            )

            self.output_parameters = self.WorkflowOutput(response=self.tasks[0].output_ref("response"))

    class GetScheduleWorkflow(WorkflowImpl):
        name: str = "Get_schedule_by_name"
        version: int = 1
        description: str = "Get scheduled workflow by name"
        labels: list[str] = ["SCHELLAR"]

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            name: WorkflowInputField = WorkflowInputField(
                name="name",
                frontend_default_value="",
                description="Schedule name",
                type=FrontendWFInputFieldType.STRING,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            self.tasks.append(
                SimpleTask(
                    name=Schellar.GetSchedule,
                    task_reference_name="schedule_by_name",
                    input_parameters=SimpleTaskInputParameters(root=dict(name=workflow_inputs.name.wf_input)),
                )
            )

            self.output_parameters = self.WorkflowOutput(response=self.tasks[0].output_ref("response"))

    class GetSchedulesWorkflow(WorkflowImpl):
        name: str = "Get_schedules"
        version: int = 1
        description: str = "Get schedules"
        labels: list[str] = ["SCHELLAR"]

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            workflow_name: WorkflowInputField = WorkflowInputField(
                name="workflow_name",
                frontend_default_value="",
                description="Existing workflow name",
                type=FrontendWFInputFieldType.STRING,
            )

            workflow_version: WorkflowInputField = WorkflowInputField(
                name="workflow_version",
                frontend_default_value="",
                description="Existing workflow version",
                type=FrontendWFInputFieldType.STRING,
            )

            cursor: WorkflowInputField = WorkflowInputField(
                name="cursor",
                frontend_default_value="",
                description="Pagination cursor",
                type=FrontendWFInputFieldType.STRING,
            )

            size: WorkflowInputField = WorkflowInputField(
                name="size",
                frontend_default_value=10,
                description="Pagination size",
                type=FrontendWFInputFieldType.INT,
            )

            type: WorkflowInputField = WorkflowInputField(
                name="type",
                frontend_default_value=PaginationCursorType.NONE,
                description="Pagination type",
                options=[item for item in PaginationCursorType],
                type=FrontendWFInputFieldType.SELECT,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            response: str

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            self.tasks.append(
                SimpleTask(
                    name=Schellar.GetSchedules,
                    task_reference_name="schedule_by_name",
                    input_parameters=SimpleTaskInputParameters(
                        root=dict(
                            workflow_name=workflow_inputs.workflow_name.wf_input,
                            workflow_version=workflow_inputs.workflow_version.wf_input,
                            cursor=workflow_inputs.cursor.wf_input,
                            size=workflow_inputs.size.wf_input,
                            type=workflow_inputs.type.wf_input,
                        )
                    ),
                )
            )

            self.output_parameters = self.WorkflowOutput(response=self.tasks[0].output_ref("response"))
