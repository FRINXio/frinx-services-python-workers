import json
from typing import Any

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task import Task
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl

from .producer_cache import KafkaProducerCache
from .producer_cache import ProducerConfigCommon
from .producer_cache import ProducerConfigSaslPlain
from .producer_cache import ProducerConfigServers
from .producer_cache import ProducerConfigSSL
from .producer_cache import SecurityProtocolType

kafka_producer_cache = KafkaProducerCache()


class KafkaWorker(ServiceWorkersImpl):
    class KafkaPublish(WorkerImpl):

        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name = 'Kafka_publish'
            description = 'Publish message to kafka broker'
            labels = ['KAFKA']
            timeout_seconds = 60
            response_timeout_seconds = 60

        class WorkerInput(TaskInput):
            bootstrap_servers: str
            topic: str
            message: Any
            key: Any
            security: str
            ssl_conf: Any

        class WorkerOutput(TaskOutput):
            ...

        def execute(self, task: Task) -> TaskResult:
            config = ProducerConfigServers(bootstrap_servers=task.input_data.get('bootstrap_servers', '')).dict(
                by_alias=True, exclude_none=True)

            match SecurityProtocolType(task.input_data.get('security')):
                case SecurityProtocolType.SSL:
                    ssl_conf = ProducerConfigSSL(**json.loads(task.input_data.get('ssl_conf', ''))).dict(
                        by_alias=True, exclude_none=True)
                    config = config | ssl_conf
                case SecurityProtocolType.SASL_PLAINTEXT:
                    ssl_conf = ProducerConfigSaslPlain(**json.loads(task.input_data.get('ssl_conf', ''))).dict(
                        by_alias=True, exclude_none=True)
                    config = config | ssl_conf
                case SecurityProtocolType.SASL_SSL:
                    ssl_conf = ProducerConfigCommon(**json.loads(task.input_data.get('ssl_conf', ''))).dict(
                        by_alias=True, exclude_none=True)
                    config = config | ssl_conf

            producer = kafka_producer_cache.get_producer(config)

            producer.send(
                topic=task.input_data.get('topic', None),
                value=task.input_data.get('message', None).encode('utf-8'),
                key=task.input_data.get('key', None).encode('utf-8')
            )
            return TaskResult(status=TaskResultStatus.COMPLETED, logs='Kafka message published successfully')
