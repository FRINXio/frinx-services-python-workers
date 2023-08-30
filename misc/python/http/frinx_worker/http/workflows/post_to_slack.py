from frinx.common.conductor_enums import WorkflowStatus
from frinx.common.type_aliases import ListStr
from frinx.common.workflow.service import ServiceWorkflowsImpl
from frinx.common.workflow.task import SimpleTask
from frinx.common.workflow.task import SimpleTaskInputParameters
from frinx.common.workflow.workflow import FrontendWFInputFieldType
from frinx.common.workflow.workflow import WorkflowImpl
from frinx.common.workflow.workflow import WorkflowInputField

from .. import HTTPWorkersService


class PostToSlackService(ServiceWorkflowsImpl):
    class PostToSlackV1(WorkflowImpl):
        name: str = 'Post_to_Slack'
        version: int = 1
        description: str = 'Post a message to your favorite Slack channel'
        restartable: bool = True
        labels: ListStr = ['HTTP', 'SLACK']

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            slack_webhook_id: WorkflowInputField = WorkflowInputField(
                name='slack_webhook_id',
                description='The Slack webhook ID that you want to send this message to',
                frontend_default_value=None,
                type=FrontendWFInputFieldType.STRING
            )
            message_text: WorkflowInputField = WorkflowInputField(
                name='message_text',
                description='The message that you want to send to Slack',
                frontend_default_value=None,
                type=FrontendWFInputFieldType.TEXTAREA
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            status: WorkflowStatus

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            worker_input = SimpleTaskInputParameters(
                uri=f'https://hooks.slack.com/services/{workflow_inputs.slack_webhook_id.wf_input}',
                body={'text': workflow_inputs.message_text.wf_input},
                method='POST'
            )

            self.tasks.append(
                SimpleTask(
                    name=HTTPWorkersService.GenericHTTPWorker,
                    task_reference_name='HTTP_SLACK',
                    input_parameters=worker_input
                )
            )
