from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Optional

from anyio import create_memory_object_stream, create_task_group
from anyio.abc import AsyncResource, CancelScope, TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..async_node import AsyncNode
from ..model import EdgeModel, FlowModel


class Flow(AsyncResource):
    """A set of tasks, representing a flow in an app."""

    def __init__(self, model: FlowModel):
        """Create a Flow from a FlowModel."""
        self._model: FlowModel = model
        self._stack = AsyncExitStack()
        self._tg: Optional[TaskGroup] = None
        # Buffers
        self._senders: dict[EdgeModel, MemoryObjectSendStream] = {}
        self._receivers: dict[EdgeModel, MemoryObjectReceiveStream] = {}

    async def __aenter__(self):
        """Start the flow in a taskgroup."""
        # Create buffers
        for edge_model in self._model.edges:
            send_stream, receive_stream = create_memory_object_stream(
                max_buffer_size=20
            )  # TODO: add item_types
            self._senders[edge_model] = send_stream
            self._receivers[edge_model] = receive_stream

        # Create Tasks
        nodes = []
        for node_name in self._model.nodes.keys():
            # Create new node
            node = AsyncNode(
                node_name, self._model.nodes[node_name], self._senders, self._receivers
            )
            # Put on the stack
            await self._stack.enter_async_context(node)
            nodes.append(node)

        # Create task group and start tasks
        self._tg = create_task_group()
        await self._stack.enter_async_context(self._tg)
        for node in nodes:
            # Start task
            await self._tg.start(node)

        return self

    async def aclose(self):
        """Close a flow. Stop all computation."""
        await self._tg.cancel_scope.cancel()
        await self._stack.aclose()


@dataclass
class FlowCancelScope:
    """Wraps (potentially) several different cancel scopes relating to a flow."""

    all_tasks: CancelScope

    async def cancel_all(self):
        """Cancel all tasks."""
        await self.all_tasks.cancel()


async def run_flow_in_taskgroup(flow: Flow, tg: TaskGroup) -> FlowCancelScope:
    """Start tasks from a flow. Returns a cancellation object."""
    pass  # TODO!
