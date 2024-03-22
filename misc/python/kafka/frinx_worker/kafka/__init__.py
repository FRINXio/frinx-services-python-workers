import json
from typing import Any

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.worker.service import ServiceWorkersImpl
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
            name: str = "Kafka_publish"
            description: str = "Publish message to kafka broker"
            labels: list[str] = ["KAFKA"]
            timeout_seconds: int = 60
            response_timeout_seconds: int = 60

        class WorkerInput(TaskInput):
            bootstrap_servers: str
            topic: str
            message: Any
            key: Any
            security: str
            ssl_conf: Any

        class WorkerOutput(TaskOutput): ...

        def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
            config = ProducerConfigServers(bootstrap_servers=worker_input.bootstrap_servers).dict(
                by_alias=True, exclude_none=True
            )

            match SecurityProtocolType(worker_input.security):
                case SecurityProtocolType.SSL:
                    ssl_conf = ProducerConfigSSL(**json.loads(worker_input.ssl_conf)).dict(
                        by_alias=True, exclude_none=True
                    )
                    config = config | ssl_conf
                case SecurityProtocolType.SASL_PLAINTEXT:
                    ssl_conf = ProducerConfigSaslPlain(**json.loads(worker_input.ssl_conf)).dict(
                        by_alias=True, exclude_none=True
                    )
                    config = config | ssl_conf
                case SecurityProtocolType.SASL_SSL:
                    ssl_conf = ProducerConfigCommon(**json.loads(worker_input.ssl_conf)).dict(
                        by_alias=True, exclude_none=True
                    )
                    config = config | ssl_conf

            producer = kafka_producer_cache.get_producer(config)

            producer.send(
                topic=worker_input.topic,
                value=worker_input.message.encode("utf-8"),
                key=worker_input.key.encode("utf-8"),
            )
            return TaskResult(status=TaskResultStatus.COMPLETED, logs="Kafka message published successfully")
