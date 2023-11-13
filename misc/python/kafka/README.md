# Conductor Kafka producer worker for Frinx Machine

Conductor Kafka producer workers for Frinx Machine

## Include

- Kafka_publish worker
- Generic Kafka producer workflow

### Prerequisites

- Python 3.10+ is required to use this package.


### Workflow input example

```json
{
  ...
  "tasks": [
    {
      "name": "Kafka_publish",
      "taskReferenceName": "kafka_publish",
      "type": "SIMPLE",
      "inputParameters": {
        "topic": "${workflow.input.topic}",
        "key": "${workflow.input.key}",
        "message": "${workflow.input.message}",
        "bootstrap_servers": "kafka:9092",
        "security": "SASL_PLAINTEXT",
        "ssl_conf": "{\"sasl_plain_username\": \"user1\", \"sasl_plain_password\": \"password1\", \"sasl_mechanism\": \"PLAIN\"}"
      }
    }
  ]
  ...
}
```
