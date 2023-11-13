import hashlib
import json
import logging
import os
from enum import Enum
from typing import Any
from typing import Optional

import cache3
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator

logger = logging.getLogger(__name__)


def construct_full_path(input_string: str, read_file: bool = False) -> tuple[str, Optional[str]]:
    try:
        absolute_path_env = os.environ.get(input_string.split('.')[0][2:-1])

        if not absolute_path_env:
            full_path = input_string
        else:
            suffix = input_string[len(input_string.split('.')[0]) + 1:]
            full_path = os.path.join(absolute_path_env, suffix)

        if read_file:
            with open(full_path) as file:
                file_content = file.read()
                return full_path, file_content

        return full_path, None

    except Exception as error:
        logging.exception(f'Transformation of secret failed: {error}')
        raise Exception(f'Transformation of secret failed: {error}')


class SecurityProtocolType(str, Enum):
    PLAINTEXT = 'PLAINTEXT'
    SSL = 'SSL'
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'


class ProducerConfigCommon(BaseModel):
    bootstrap_servers: Optional[str] = None
    client_id: Optional[str] = None
    key_serializer: Optional[str] = None
    value_serializer: Optional[str] = None
    acks: Optional[int] = None
    compression_type: Optional[str] = None
    retries: Optional[int] = None
    batch_size: Optional[int] = None
    linger_ms: Optional[int] = None
    buffer_memory: Optional[int] = None
    connections_max_idle_ms: Optional[int] = None
    max_block_ms: Optional[int] = None
    max_request_size: Optional[int] = None
    metadata_max_age_ms: Optional[int] = None
    retry_backoff_ms: Optional[int] = None
    request_timeout_ms: Optional[int] = None
    receive_buffer_bytes: Optional[str] = None
    send_buffer_bytes: Optional[str] = None
    socket_options: Optional[list[Any]] = None
    sock_chunk_bytes: Optional[int] = None
    reconnect_backoff_max_ms: Optional[int] = None
    max_in_flight_requests_per_connection: Optional[int] = None
    security_protocol: Optional[str] = None
    ssl_context: Optional[str] = None
    ssl_check_hostname: Optional[bool] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_crlfile: Optional[str] = None
    ssl_password: Optional[str] = None
    ssl_ciphers: Optional[str] = None
    api_version: Optional[str] = None
    api_version_auto_timeout_ms: Optional[int] = None
    metric_reporters: Optional[list[Any]] = None
    metrics_num_samples: Optional[int] = None
    metrics_sample_window_ms: Optional[int] = None
    selector: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    sasl_kerberos_service_name: Optional[str] = None
    sasl_kerberos_domain_name: Optional[str] = None
    sasl_oauth_token_provider: Optional[str] = None


class ProducerConfigServers(ProducerConfigCommon):
    bootstrap_servers: str


class ProducerConfigSSL(ProducerConfigCommon):
    ssl_check_hostname: bool
    ssl_cafile: str
    ssl_certfile: str
    ssl_keyfile: str
    ssl_password: str
    security_protocol: SecurityProtocolType = SecurityProtocolType.SSL

    model_config = ConfigDict(
        use_enum_values=True
    )

    @field_validator('ssl_cafile', 'ssl_certfile', 'ssl_keyfile')
    def check_env(cls, v: str) -> str:
        return construct_full_path(v)[0]

    @field_validator('ssl_password')
    def read_passwd(cls, v: str) -> Any:
        return construct_full_path(v, True)[1]


class ProducerConfigSaslPlain(ProducerConfigCommon):
    sasl_plain_username: str
    sasl_plain_password: str
    sasl_mechanism: str
    security_protocol: SecurityProtocolType = SecurityProtocolType.SASL_PLAINTEXT

    model_config = ConfigDict(
        use_enum_values=True
    )


class KafkaProducerCache:
    __ttl: int
    __clients: cache3.Cache = cache3.Cache(name='Kafka client')

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
                logging.info(f'Creating kafka client {client_id}')
                self.__clients.set(key=client_id, timeout=self.__ttl, value=KafkaProducer(**client_config))
            self.__clients.touch(key=client_id, timeout=self.__ttl)
            return self.__clients.get(key=client_id)

        except KafkaError as error:
            logging.error(f'{self.__class__.__name__}: Failed to create Kafka client for {client_id}: {error}')
            raise Exception(f'{self.__class__.__name__}: Failed to create Kafka client for {client_id}: {error}')

        except Exception as error:
            logging.exception(f'{self.__class__.__name__}: Unknown error: {error}')
            raise Exception(f'{self.__class__.__name__}: Unknown error: {error}')
