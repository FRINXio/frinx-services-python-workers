from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Optional

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from pydantic import BaseModel


class QueueInfo(BaseModel):
    parent_wf_id: str
    parent_wf_name: str
    resources_locked: list[str]


class QueueInfoWithRoot(BaseModel):
    root_wf_id: str
    root_wf_name: str
    service_order_id: int | None
    resources_locked: list[str]


class QueueStatus(StrEnum):
    PROCESSING = TaskResultStatus.IN_PROGRESS
    COMPLETED = TaskResultStatus.COMPLETED
    FAILED = TaskResultStatus.FAILED


class TaskResponse(BaseModel):
    task: DictAny
    status: QueueStatus
    source_workflow_id: Optional[str]
    response_body: Optional[str | DictAny]


class Resource(BaseModel):
    name: str
    limit: int
    used: int

    def __hash__(self) -> int:
        return hash(self.__dict__.values())

    def __add__(self, other: Resource) -> Resource:
        if not isinstance(other, self.__class__):
            raise TypeError(
                f'Cannot perform addition of instance of {self.__class__} with the instance o'
                f'f {type(other)}.'
            )
        if self.name != other.name:
            raise ValueError(
                f'Cannot add instances of different resources ({self.name} != {other.name}).'
            )
        new_used = self.used + other.used
        new_limit = max(self.limit, other.limit)
        if new_used > new_limit:
            raise ValueError(f'The sum {new_used} is over the limit {new_limit}.')
        return Resource(name=self.name, limit=new_limit, used=new_used)

    def __sub__(self, other: Resource) -> Resource:
        if not isinstance(other, self.__class__):
            raise TypeError(
                f'Cannot perform subtraction of instance of {self.__class__} with the instance'
                f' of {type(other)}.'
            )
        if self.name != other.name:
            raise ValueError(
                f'Cannot subtract instances of different resources ({self.name} != {other.name}).'
            )
        new_used = self.used - other.used
        new_limit = max(self.limit, other.limit)
        if new_used < 0:
            raise ValueError(f'The difference {new_used} is below zero.')
        return Resource(name=self.name, limit=new_limit, used=new_used)


class Queue(BaseModel):
    task_id: str
    task: DictAny
    queue_workflow_id: str
    source_workflow_id: str
    workflow_path: list[str]
    dependencies: Optional[list[str]]
    resources: dict[str, Resource]
    resources_context: dict[str, Resource]
    status: QueueStatus
    execution_start: Optional[datetime]
    response_body: Optional[str | DictAny]

    @property
    def resource_diff(self) -> dict[str, Resource]:
        """Returns non-zero differences of resources and resources_context above  `Resource.used` values"""
        diff = set()
        for resource_name, resource in self.resources.items():
            try:
                diff.add(
                    resource
                    - self.resources_context.get(
                        resource_name, Resource(name=resource_name, limit=0, used=0)
                    )
                )
            except ValueError:
                pass
        return {resource.name: resource for resource in diff if resource.used > 0}

    @property
    def resource_union(self) -> dict[str, Resource]:
        """Returns union of resources and resources_context maximising `Resource.limit` and `Resource.used` values"""
        union = set()
        for resource_name, resource in self.resources.items():
            if resource_name not in self.resources_context:
                union.add(resource)
                continue
            context_resource = self.resources_context[resource_name]
            union.add(
                Resource(
                    name=resource_name,
                    limit=max(resource.limit, context_resource.limit),
                    used=max(resource.used, context_resource.used),
                )
            )
        for resource_name, resource in self.resources_context.items():
            if resource_name not in self.resources:
                union.add(resource)
        return {resource.name: resource for resource in union}

    def __hash__(self) -> int:
        return hash(self.task_id)

    def emit_response(self) -> TaskResponse:
        if self.status == QueueStatus.COMPLETED:

            if all(
                [
                    (resource.limit == 1 and resource.used == 1)
                    for resource in self.resource_union.values()
                ]
            ):
                output_resources_context = [
                    resource.name for resource in self.resource_union.values()
                ]
            else:
                output_resources_context = [
                    dict(resource) for resource in self.resource_union.values()  # type: ignore[misc]
                ]
            self.response_body = {'resources_context': output_resources_context}

        return TaskResponse(
            task=self.task,
            status=self.status,
            source_workflow_id=self.source_workflow_id,
            response_body=self.response_body,
        )


class QueueRelease(BaseModel):
    task_id: str
    task: DictAny
    queue_workflow_id: str
    queue_task_id: Optional[str]
    queue: Optional[Queue]

    def emit_response(self, status: QueueStatus, response_body: str | DictAny) -> TaskResponse:
        return TaskResponse(task=self.task, status=status, response_body=response_body)
