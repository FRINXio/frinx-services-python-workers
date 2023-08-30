from enum import Enum


class HttpMethod(str, Enum):
    CONNECT = 'CONNECT'
    OPTIONS = 'OPTIONS'
    TRACE = 'TRACE'
    DELETE = 'DELETE'
    PATCH = 'PATCH'
    HEAD = 'HEAD'
    POST = 'POST'
    PUT = 'PUT'
    GET = 'GET'


class ContentType(str, Enum):
    APPLICATION_JSON = 'application/json'
    APPLICATION_XML = 'application/xml'
    APPLICATION_X_WWW_FORM_URLENCODED = 'application/x-www-form-urlencoded'
    MULTIPART_FROM_DATA = 'multipart/form-data'
    TEXT_CSV = 'text/csv'
    TEXT_PLAIN = 'text/plain'
    TEXT_XML = 'text/xml'
