from frinx.common.conductor_enums import WorkflowStatus
from frinx.common.type_aliases import ListAny
from frinx.common.workflow.service import ServiceWorkflowsImpl
from frinx.common.workflow.task import SimpleTask
from frinx.common.workflow.task import SimpleTaskInputParameters
from frinx.common.workflow.workflow import FrontendWFInputFieldType
from frinx.common.workflow.workflow import WorkflowImpl
from frinx.common.workflow.workflow import WorkflowInputField

from .. import HTTPWorkersService
from ..enums import ContentType
from ..enums import HttpMethod


class GenericRequestService(ServiceWorkflowsImpl):

    class GenericRequestV1(WorkflowImpl):
        name: str = 'Http_request'
        version: int = 1
        description: str = 'Generic HTTP request'
        restartable: bool = True
        labels: ListAny = ['HTTP', 'GENERIC']

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            uri: WorkflowInputField = WorkflowInputField(
                name='uri',
                frontend_default_value=None,
                description='Request url.',
                type=FrontendWFInputFieldType.STRING,
            )
            method: WorkflowInputField = WorkflowInputField(
                name='method',
                frontend_default_value='GET',
                description='Request method.',
                options=list(HttpMethod),
                type=FrontendWFInputFieldType.SELECT,
            )
            content_type: WorkflowInputField = WorkflowInputField(
                name='contentType',
                frontend_default_value='application/json',
                description='Request Content-Type header.',
                options=list(ContentType),
                type=FrontendWFInputFieldType.SELECT,
            )
            connect_timeout: WorkflowInputField = WorkflowInputField(
                name='connectTimeout',
                frontend_default_value=360,
                description='Timeout of creating connection to server.',
                type=FrontendWFInputFieldType.INT,
            )
            read_timeout: WorkflowInputField = WorkflowInputField(
                name='readTimeout',
                frontend_default_value=360,
                description='Timeout of receiving server response.',
                type=FrontendWFInputFieldType.INT,
            )
            body: WorkflowInputField = WorkflowInputField(
                name='body',
                frontend_default_value=None,
                description='Request body.',
                type=FrontendWFInputFieldType.TEXTAREA,
            )
            basic_auth: WorkflowInputField = WorkflowInputField(
                name='basicAuth',
                frontend_default_value=None,
                description='Basic authentication, consist of username and password.',
                type=FrontendWFInputFieldType.TEXTAREA,
            )
            headers: WorkflowInputField = WorkflowInputField(
                name='headers',
                frontend_default_value=None,
                description='Request headers',
                type=FrontendWFInputFieldType.TEXTAREA,
            )
            cookies: WorkflowInputField = WorkflowInputField(
                name='cookies',
                frontend_default_value=None,
                description='Incoming cookies to the request.',
                type=FrontendWFInputFieldType.TEXTAREA,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            status: WorkflowStatus

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            worker_input = SimpleTaskInputParameters(
                root=dict(
                    uri=workflow_inputs.uri.wf_input,
                    method=workflow_inputs.method.wf_input,
                    connect_timeout=workflow_inputs.connect_timeout.wf_input,
                    read_timeout=workflow_inputs.read_timeout.wf_input,
                    headers=workflow_inputs.headers.wf_input,
                    content_type=workflow_inputs.content_type.wf_input,
                    body=workflow_inputs.body.wf_input,
                    cookies=workflow_inputs.cookies.wf_input,
                    basic_auth=workflow_inputs.basic_auth.wf_input
                )
            )

            self.tasks.append(
                SimpleTask(
                    name=HTTPWorkersService.GenericHTTPWorker,
                    task_reference_name='HTTP_GENERIC',
                    input_parameters=worker_input,
                )
            )
