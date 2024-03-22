import hashlib
import json
import logging
import os
from enum import Enum
from typing import Any

import cache3
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator

logger = logging.getLogger(__name__)


def construct_full_path(input_string: str, read_file: bool = False) -> tuple[str, str | None]:
    try:
        absolute_path_env = os.environ.get(input_string.split(".")[0][2:-1])

        if not absolute_path_env:
            full_path = input_string
        else:
            suffix = input_string[len(input_string.split(".")[0]) + 1 :]
            full_path = os.path.join(absolute_path_env, suffix)

        if read_file:
            with open(full_path) as file:
                file_content = file.read()
                return full_path, file_content

        return full_path, None

    except Exception as error:
        logging.exception(f"Transformation of secret failed: {error}")
        raise Exception(f"Transformation of secret failed: {error}")


class SecurityProtocolType(str, Enum):
    PLAINTEXT = "PLAINTEXT"
    SSL = "SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


class ProducerConfigCommon(BaseModel):
    bootstrap_servers: str | None = None
    client_id: str | None = None
    key_serializer: str | None = None
    value_serializer: str | None = None
    acks: int | None = None
    compression_type: str | None = None
    retries: int | None = None
    batch_size: int | None = None
    linger_ms: int | None = None
    buffer_memory: int | None = None
    connections_max_idle_ms: int | None = None
    max_block_ms: int | None = None
    max_request_size: int | None = None
    metadata_max_age_ms: int | None = None
    retry_backoff_ms: int | None = None
    request_timeout_ms: int | None = None
    receive_buffer_bytes: str | None = None
    send_buffer_bytes: str | None = None
    socket_options: list[Any] | None = None
    sock_chunk_bytes: int | None = None
    reconnect_backoff_max_ms: int | None = None
    max_in_flight_requests_per_connection: int | None = None
    security_protocol: str | None = None
    ssl_context: str | None = None
    ssl_check_hostname: bool | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_crlfile: str | None = None
    ssl_password: str | None = None
    ssl_ciphers: str | None = None
    api_version: str | None = None
    api_version_auto_timeout_ms: int | None = None
    metric_reporters: list[Any] | None = None
    metrics_num_samples: int | None = None
    metrics_sample_window_ms: int | None = None
    selector: str | None = None
    sasl_mechanism: str | None = None
    sasl_plain_username: str | None = None
    sasl_plain_password: str | None = None
    sasl_kerberos_service_name: str | None = None
    sasl_kerberos_domain_name: str | None = None
    sasl_oauth_token_provider: str | None = None


class ProducerConfigServers(ProducerConfigCommon):
    bootstrap_servers: str


class ProducerConfigSSL(ProducerConfigCommon):
    ssl_check_hostname: bool
    ssl_cafile: str
    ssl_certfile: str
    ssl_keyfile: str
    ssl_password: str
    security_protocol: SecurityProtocolType = SecurityProtocolType.SSL

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("ssl_cafile", "ssl_certfile", "ssl_keyfile")
    def check_env(cls, v: str) -> str:
        return construct_full_path(v)[0]

    @field_validator("ssl_password")
    def read_passwd(cls, v: str) -> Any:
        return construct_full_path(v, True)[1]


class ProducerConfigSaslPlain(ProducerConfigCommon):
    sasl_plain_username: str
    sasl_plain_password: str
    sasl_mechanism: str
    security_protocol: SecurityProtocolType = SecurityProtocolType.SASL_PLAINTEXT

    model_config = ConfigDict(use_enum_values=True)


class KafkaProducerCache:
    __ttl: int
    __clients: cache3.Cache = cache3.Cache(name="Kafka client")

    def __init__(self, cache_lifetime: int = 300):
        self.__ttl: int = cache_lifetime

    @staticmethod
    def unique_id(kafka_producer_config: dict[str, Any]) -> str:
        return hashlib.md5(json.dumps(kafka_producer_config, sort_keys=True).encode()).hexdigest()

    def get_producer(self, kafka_producer_config: dict[str, Any]) -> KafkaProducer:
        client_config = ProducerConfigCommon(**kafka_producer_config).dict(exclude_none=True)
        client_id = self.unique_id(client_config)
        try:
            if client_id not in self.__clients:
                logging.info(f"Creating kafka client {client_id}")
                self.__clients.set(key=client_id, timeout=self.__ttl, value=KafkaProducer(**client_config))
            self.__clients.touch(key=client_id, timeout=self.__ttl)
            return self.__clients.get(key=client_id)

        except KafkaError as error:
            logging.error(f"{self.__class__.__name__}: Failed to create Kafka client for {client_id}: {error}")
            raise Exception(f"{self.__class__.__name__}: Failed to create Kafka client for {client_id}: {error}")

        except Exception as error:
            logging.exception(f"{self.__class__.__name__}: Unknown error: {error}")
            raise Exception(f"{self.__class__.__name__}: Unknown error: {error}")
