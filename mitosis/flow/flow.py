from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Optional

from anyio import create_memory_object_stream, create_task_group
from anyio.abc import AsyncResource, CancelScope, TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..async_node import AsyncNode
from ..model import EdgeModel, FlowModel


class Flow(AsyncResource):
    """
    A set of tasks, representing a flow in an app.

    The current usage is to create the object, use it in the run_in_task_group function,
    and then at a later point stop it through the FlowHandle.
    """

    def __init__(self, model: FlowModel):
        """Create a Flow from a FlowModel."""
        self._model: FlowModel = model
        self._stack = AsyncExitStack()
        self._tg: Optional[TaskGroup] = None
        # Buffers
        self._senders: dict[EdgeModel, MemoryObjectSendStream] = {}
        self._receivers: dict[EdgeModel, MemoryObjectReceiveStream] = {}

        # Create buffers
        for edge_model in self._model.edges:
            send_stream, receive_stream = create_memory_object_stream(
                max_buffer_size=20
            )  # TODO: add item_types
            self._senders[edge_model] = send_stream
            self._receivers[edge_model] = receive_stream

    async def aclose(self):
        """Close a flow."""
        await self._stack.aclose()


@dataclass
class FlowCancelScope:
    """Wraps (potentially) several different cancel scopes relating to a flow."""

    all_tasks: CancelScope

    async def cancel_all(self):
        """Cancel all tasks."""
        await self.all_tasks.cancel()


@dataclass
class FlowHandle:
    """A Flow and its FlowCancelScope, with convenience methods."""

    flow: Flow
    scopes: FlowCancelScope

    async def stop_flow_immediately(self):
        """Stop all tasks and unwind stack."""
        await self.scopes.cancel_all()
        await self.flow.aclose()


async def run_flow_in_taskgroup(flow: Flow, tg: TaskGroup) -> FlowHandle:
    """Start tasks from a flow. Returns a cancellation object."""
    # Create Tasks
    nodes = []
    for node_name in flow._model.nodes.keys():
        # Create new node
        node = AsyncNode(
            node_name, flow._model.nodes[node_name], flow._senders, flow._receivers
        )
        # Put on the stack
        await flow._stack.enter_async_context(node)
        nodes.append(node)

    # Start tasks
    with CancelScope() as scope:
        for node in nodes:
            # Start task
            await tg.start(node)

    flow_cancel_scope = FlowCancelScope(scope)
    return FlowHandle(flow, flow_cancel_scope)
