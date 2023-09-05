from collections import defaultdict
from collections.abc import Iterator
from typing import Any
from typing import Optional
from typing import Union

from .models import Queue


class QueueCollection:
    def __init__(self) -> None:
        self.queues: dict[str, Queue] = {}
        self.ordered: list[str] = []

    def __len__(self) -> int:
        return len(self.ordered)

    def __contains__(self, task_id: str) -> bool:
        return task_id in self.queues

    def __iter__(self) -> Iterator[Queue]:
        for task_id in self.ordered:
            yield self.queues[task_id]

    def __getitem__(self, key: Any) -> Union[Queue, list[Queue]]:
        if isinstance(key, slice):
            start, stop, step = key.start, key.stop, key.step
            if start is None:
                start = 0
            if stop is None:
                stop = len(self)
            if step is None:
                step = 1
            queues_slice = [self.queues[self.ordered[i]] for i in range(start, stop, step)]
            return queues_slice
        return self.queues[key]

    def insert(self, queue: Queue) -> None:
        self.queues[queue.task_id] = queue
        self.ordered.append(queue.task_id)

    def remove(self, task_id: str) -> None:
        self.ordered.remove(task_id)
        del self.queues[task_id]

    def pop_by_wf_id(self, wf_id: str) -> Queue:
        for queue in self.queues.values():
            if wf_id == queue.queue_workflow_id:
                self.ordered.remove(queue.task_id)
                del self.queues[queue.task_id]
                return queue
        raise KeyError('Queue not found.')

    def pop(self, task_id: Optional[str] = None) -> Queue:
        if not task_id:
            task_id = self.ordered[0]
        self.ordered.remove(task_id)
        return self.queues.pop(task_id)

    def update_item(self, task_id: str, **kwargs: Any) -> None:
        for field, value in kwargs.items():
            setattr(self.queues[task_id], field, value)


class ResourceAllocationGraph:
    """Represents relationships between workflows and resources.

    Workflow W waiting for the resource R (resource request) is represented by an edge from the node W to the node R.
    Workflow W using the resource R (resource allocation) is represented by an edge from the node R to the node W.
    Workflow S being a subworkflow of the workflow W (dependency) is represented by an edge from the node W to the
    node S.

    Implements a BFS-based method for finding the shortest cycle, thus detecting a deadlock."""

    def __init__(self) -> None:
        self.graph: dict[str, dict[str, list[str]]] = defaultdict(
            lambda: {'outgoing': [], 'incoming': []}
        )

    def add_workflow(self, workflow_id: str) -> None:
        if workflow_id not in self.graph:
            self.graph[workflow_id] = {'outgoing': [], 'incoming': []}

    def add_resource(self, resource_name: str) -> None:
        if resource_name not in self.graph:
            self.graph[resource_name] = {'outgoing': [], 'incoming': []}

    def add_edge(self, from_: str, to_: str) -> None:
        if from_ not in self.graph:
            self.graph[from_] = {'outgoing': [], 'incoming': []}
        if to_ not in self.graph:
            self.graph[to_] = {'outgoing': [], 'incoming': []}

        self.graph[from_]['outgoing'].append(to_)
        self.graph[to_]['incoming'].append(from_)

    def remove_edge(self, from_: str, to_: str) -> None:
        if from_ in self.graph and to_ in self.graph[from_]['outgoing']:
            self.graph[from_]['outgoing'].remove(to_)
        if to_ in self.graph and from_ in self.graph[to_]['incoming']:
            self.graph[to_]['incoming'].remove(from_)

    def remove_resource_request_edge(self, workflow_id: str, resource_name: str) -> None:
        self.remove_edge(workflow_id, resource_name)

    def remove_resource_allocation_edge(self, workflow_id: str, resource_name: str) -> None:
        self.remove_edge(resource_name, workflow_id)

    def add_resource_allocation_edge(self, workflow_id: str, resource_name: str) -> None:
        self.add_edge(resource_name, workflow_id)

    def add_resource_request_edge(self, workflow_id: str, resource_name: str) -> None:
        self.add_edge(workflow_id, resource_name)

    def add_workflow_dependency_edge(self, parent_workflow_id: str, subworkflow_id: str) -> None:
        self.add_edge(parent_workflow_id, subworkflow_id)

    def allocate_resource(self, workflow_id: str, resource_name: str) -> None:
        # First cancel the request
        self.remove_resource_request_edge(workflow_id, resource_name)
        # Then allocate
        self.add_resource_allocation_edge(workflow_id, resource_name)

    def cancel_requests_of_workflow(self, workflow_id: str) -> None:
        for resource_name in self.graph[workflow_id]['outgoing'][:]:
            self.remove_resource_request_edge(workflow_id, resource_name)

    def remove_workflow(self, workflow_id: str) -> None:
        if workflow_id in self.graph:
            # Cancel all requests of the workflow
            self.cancel_requests_of_workflow(workflow_id)

            # Free all allocations of the workflow
            # This also removes dependency edges from the ancestral workflows
            for resource_name in self.graph[workflow_id]['incoming'][:]:
                self.remove_edge(resource_name, workflow_id)

            # Remove workflow and its dependencies from the graph
            del self.graph[workflow_id]

    def find_cycle(self, workflow_id: str) -> list[str]:
        if workflow_id not in self.graph:
            raise ValueError(f"Workflow '{workflow_id}' not found in the graph.")

        queue = [(workflow_id, [workflow_id])]
        visited = {workflow_id}

        while queue:
            node, path = queue.pop(0)

            for neighbor in self.graph[node]['outgoing']:
                if neighbor == workflow_id:
                    return path + [neighbor]

                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))

        return []
