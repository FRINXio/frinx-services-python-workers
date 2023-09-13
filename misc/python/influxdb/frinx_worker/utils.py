from typing import Any
from typing import Optional
import json
from datetime import datetime
from typing import Any

from frinx.common.frinx_rest import INFLUXDB_URL_BASE
from influxdb_client import InfluxDBClient
from pydantic import BaseModel


from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDbWrapper:
    def __init__(self, token: str, org: str) -> None:
        # TODO change to influxdb:8086
        self.url = "http://localhost:8086"
        self.token = token
        self.org = org

    def client(self) -> InfluxDBClient:
        return InfluxDBClient(url=self.url, token=self.token, org=self.org)


class InfluxOutput(BaseModel):
    code: int
    data: dict[str, Any]
    logs: Optional[list[str]] | Optional[str] = None



def influx_query_data(
    org: str, token: str, query: str, format_data: list[str] | str | None = None
) -> InfluxOutput:
    if org is None or len(org) == 0:
        raise ValueError("Bad input org %s:", org)
    if token is None or len(token) == 0:
        raise ValueError("Bad input token %s:", token)
    if query is None or len(query) == 0:
        raise ValueError("Bad input query %s:", query)

    if format_data is None or len(format_data) == 0:
        format_data = ["table", "_measurement", "_value", "host"]
    if isinstance(format_data, str):
        format_data = list(format_data.replace(" ", "").split(","))

    response = InfluxOutput(code=404, data={}, logs=None)

    with InfluxDbWrapper(token=token, org=org).client() as client:
        output = (
            client.query_api().query(json.loads(json.dumps(query))).to_values(columns=format_data)
        )

        client.close()
        response.data["output"] = json.loads(json.dumps(output))
        response.code = 200

    return response


def influx_create_bucket(org: str, token: str, bucket: str) -> InfluxOutput:
    if org is None or len(org) == 0:
        raise ValueError("Bad input org %s:", org)
    if token is None or len(token) == 0:
        raise ValueError("Bad input token %s:", token)
    if bucket is None or len(bucket) == 0:
        raise ValueError("Bad input bucket %s:", bucket)

    response = InfluxOutput(code=404, data={}, logs=None)

    with InfluxDbWrapper(token=token, org=org).client() as client:
        api = client.buckets_api()
        bucket_obj = api.find_bucket_by_name(bucket)

        if bucket_obj is not None:
            response.data["bucket"] = bucket_obj.name
            response.logs = "Bucket with this name exist before"
            response.code = 200
        else:
            bucket_api = api.create_bucket(bucket_name=bucket, org=org)
            response.data["bucket"] = bucket_api.name
            response.logs = "New bucket was created"
            response.code = 200

        client.close()

    return response


def influx_write_data(
    org: str,
    token: str,
    bucket: str,
    measurement: str,
    tags: dict[str, Any],
    fields: dict[str, Any],
) -> InfluxOutput:
    if org is None or len(org) == 0:
        raise ValueError("Bad input org %s:", org)
    if token is None or len(token) == 0:
        raise ValueError("Bad input token %s:", token)
    if bucket is None or len(bucket) == 0:
        raise ValueError("Bad input bucket %s:", bucket)

    if measurement is None or len(measurement) == 0:
        raise ValueError("Bad input measurement %s:", measurement)
    if tags is None or len(tags) == 0:
        raise ValueError("Bad input tags %s:", tags)
    if isinstance(tags, str):
        tags = json.loads(tags)
    if fields is None or len(fields) == 0:
        raise ValueError("Bad input fields %s:", fields)
    if isinstance(fields, str):
        fields = json.loads(fields)

    print(type(fields), type(tags))

    response = InfluxOutput(code=404, data={}, logs=None)

    with InfluxDbWrapper(token=token, org=org).client() as client:
        api = client.write_api(write_options=SYNCHRONOUS)

        dict_structure = {
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "time": datetime.utcnow(),
        }

        api.write(bucket, org, dict_structure)
        client.close()

    response.logs = "Successfully stored in database"
    response.code = 200

    return response