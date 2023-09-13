from datetime import datetime
from functools import partial
from http import HTTPStatus
from operator import contains

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.frinx_rest import INFLUXDB_URL_BASE
from frinx.common.worker.task_def import ConductorWorkerError
from frinx.common.worker.task_def import FailedTaskError
from frinx.common.worker.task_def import InvalidTaskInputError
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDbWrapper:
    def __init__(self, token: str, org: str) -> None:
        self.url = INFLUXDB_URL_BASE
        self.token = token
        self.org = org

    def client(self) -> InfluxDBClient:
        return InfluxDBClient(url=self.url, token=self.token, org=self.org)
    

def error_adapter(error: InfluxDBError) -> ConductorWorkerError:
        in_message = partial(contains, error.message)

        match error.response.status:
            case HTTPStatus.BAD_REQUEST:
                if in_message('undefined identifier'):
                    err_msg = f'Invalid query, {error.message}'
                elif in_message('organization name'):
                    err_msg = 'Invalid org.'
                else:
                    err_msg = error.message
                
                return InvalidTaskInputError(err_msg)
                
            case HTTPStatus.UNAUTHORIZED:
                return FailedTaskError('Unauthorized, invalid token.')
            
            case HTTPStatus.NOT_FOUND:
                if in_message('bucket'):
                    err_msg = 'Bucket not exists.'
                elif in_message('organization name'):
                    err_msg = 'Org not exists.'
                else:
                    err_msg = error.message

                return FailedTaskError(err_msg)
            
            case _:
                FailedTaskError(error.message)
    

def influx_write_data(worker: WorkerImpl, worker_input: TaskInput) -> TaskResult:
    try:
        with InfluxDbWrapper(worker_input.token, worker_input.org).client() as client:
            api = client.write_api(write_options=SYNCHRONOUS)
            api.write(
                worker_input.bucket, 
                worker_input.org, 
                record={
                    'measurement': worker_input.measurement,
                    'tags': worker_input.tags,
                    'fields': worker_input.fields,
                    'time': datetime.utcnow()
                }
            )
            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=worker.WorkerOutput(),
                logs='Successfully stored in database.'
            )
    except InfluxDBError as err:
        raise error_adapter(err)
    

def influx_query_data(worker: WorkerImpl, worker_input: TaskInput) -> TaskResult:
    default_format = ['table', '_measurement', '_value', 'host']
    try:
        with InfluxDbWrapper(worker_input.token, worker_input.org).client() as client:
            query_result = client.query_api().query(worker_input.query)
            formatted_data = query_result.to_values(worker_input.format_data or default_format)

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=worker.WorkerOutput(data=formatted_data),
                logs='Data was successfully requested from database.'
            )
    except InfluxDBError as err:
        raise error_adapter(err)


# BUG?: even if invalid org passed, BucketsApi handle connection to database
# BUG?: bucket: even if white characters passed, creation of bucket succeed
def influx_create_bucket(worker: WorkerImpl, worker_input: TaskInput) -> TaskResult:
    try:
        with InfluxDbWrapper(token=worker_input.token, org=worker_input.org).client() as client:
            api = client.buckets_api()

            if not api.find_bucket_by_name(worker_input.bucket):
                api.create_bucket(bucket_name=worker_input.bucket, org=worker_input.org)
                msg = 'Bucket was successfully created.'
            else:
                msg = 'Bucket already exists.'

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                data=worker.WorkerOutput(),
                logs=msg
            )
    except InfluxDBError as err:
        raise error_adapter(err)
