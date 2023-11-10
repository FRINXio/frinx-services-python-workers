from frinx.common.workflow.task import SimpleTask
from frinx.common.workflow.task import SimpleTaskInputParameters
from frinx.common.workflow.workflow import FrontendWFInputFieldType
from frinx.common.workflow.workflow import WorkflowImpl
from frinx.common.workflow.workflow import WorkflowInputField

from .. import KafkaWorker
from ..producer_cache import SecurityProtocolType


class KafkaProducer(WorkflowImpl):
    name: str = 'Kafka_producer'
    version: int = 1
    description: str = 'Simple Kafka producer'
    labels: list[str] = ['KAFKA']

    class WorkflowInput(WorkflowImpl.WorkflowInput):
        servers: WorkflowInputField = WorkflowInputField(
            name='servers',
            frontend_default_value='kafka:9092',
            description='Kafka bootstrap servers. Separated with coma',
            type=FrontendWFInputFieldType.STRING,
        )

        security: WorkflowInputField = WorkflowInputField(
            name='security',
            frontend_default_value='SSL',
            description='Request url',
            type=FrontendWFInputFieldType.SELECT,
            options=[str(status) for status in SecurityProtocolType]
        )

        message: WorkflowInputField = WorkflowInputField(
            name='message',
            frontend_default_value='hello telemetry &^%#$#!',
            description='Request url',
            type=FrontendWFInputFieldType.TEXTAREA,
        )

        key: WorkflowInputField = WorkflowInputField(
            name='key',
            frontend_default_value='telemetry_key',
            description='Request url',
            type=FrontendWFInputFieldType.TEXTAREA,
        )

        topic: WorkflowInputField = WorkflowInputField(
            name='topic',
            frontend_default_value='telemetry',
            description='kafka topic',
            type=FrontendWFInputFieldType.STRING,
        )

        ssl_conf: WorkflowInputField = WorkflowInputField(
            name='ssl_conf',
            frontend_default_value="""{
            "ssl_check_hostname": true,
            "ssl_cafile": "${KAFKA_SECRET}.secret.pem",
            "ssl_certfile": "${KAFKA_SECRET}.secret.pem",
            "ssl_keyfile": "${KAFKA_SECRET}.elisa.pem",
            "ssl_password": "${KAFKA_SECRET}.password"
            }""",
            description='SSL config',
            type=FrontendWFInputFieldType.TEXTAREA,
        )

    class WorkflowOutput(WorkflowImpl.WorkflowOutput):
        ...

    def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
        self.tasks.append(
            SimpleTask(
                name=KafkaWorker.KafkaPublish,
                task_reference_name='Kafka_publish',
                input_parameters=SimpleTaskInputParameters(
                    root=dict(
                        bootstrap_servers=workflow_inputs.servers.wf_input,
                        topic=workflow_inputs.topic.wf_input,
                        message=workflow_inputs.message.wf_input,
                        security=workflow_inputs.security.wf_input,
                        ssl_conf=workflow_inputs.ssl_conf.wf_input,
                        key=workflow_inputs.key.wf_input
                    )
                )
            )
        )
