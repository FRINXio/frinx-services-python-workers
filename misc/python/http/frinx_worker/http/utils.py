from collections import defaultdict

import requests
from frinx.common.worker.task_def import TaskInput
from pydantic import BaseModel


class BasicAuth(BaseModel):
    username: str
    password: str


def http_task(http_input: TaskInput) -> requests.Response:
    input_data = defaultdict(lambda: None, http_input.model_dump(by_alias=True))
    headers, timeout, json, data, auth = input_data['headers'] or {}, None, None, None, None

    if input_data['basicAuth']:
        auth = input_data['basicAuth']['username'], input_data['basicAuth']['password']

    if input_data['contentType']:
        headers['Content-Type'] = input_data['contentType']

    match input_data['connectTimeout'], input_data['readTimeout']:
        case float(), float():
            timeout = input_data['connectTimeout'], input_data['readTimeout']
        case float(), None:
            timeout = input_data['connectTimeout']

    match input_data['body']:
        case dict() | list():
            json = input_data['body']
        case str():
            data = input_data['body']

    return requests.request(
        method=input_data['method'],
        url=input_data['uri'] or input_data['url'],
        headers=headers,
        timeout=timeout or input_data['timeout'],
        data=data or input_data['data'],
        json=json or input_data['json'],
        cookies=input_data['cookies'],
        auth=auth or input_data['auth']
    )
