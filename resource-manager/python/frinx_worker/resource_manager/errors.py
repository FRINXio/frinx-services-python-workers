from frinx_api.resource_manager import ID

from frinx_worker.resource_manager.env import HELPDESK_URL


class ResourceManagerWorkerError(Exception):
    """Exception class for resource-manager workers."""


class PoolNotFoundError(ResourceManagerWorkerError, KeyError):
    """KeyError"""
    def __init__(self, pool_id: ID, *args):
        super().__init__(f'Pool with ID "{pool_id}" was not founded.', *args)


class ResourceNotFoundError(ResourceManagerWorkerError, KeyError):
    """KeyError"""
    def __init__(self, resource: str, *args):
        super().__init__(f'Resource "{resource}" was not founded.', *args)


class ResourceManagerWorkerRuntimeError(ResourceManagerWorkerError, RuntimeError):
    """RuntimeError"""
    def __init__(self):
        super().__init__(f'{self.__class__.__name__}, please report this issue on {HELPDESK_URL}. Thank you!')


class InvalidQueryError(ResourceManagerWorkerRuntimeError):
    """422 Client Error: Unprocessable Entity for url: ..."""
