from collections import defaultdict
from contextlib import AsyncExitStack
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum, auto

from anyio import create_memory_object_stream
from anyio.abc import AsyncResource, CancelScope, TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mitosis.model.edge_model import SpecificPort

from ..async_node import AsyncNode
from ..model import EdgeModel, FlowModel


@dataclass
class Flow:
    """A set of nodes, compiled from a flow model."""

    model: FlowModel
    nodes: list[AsyncNode] = field(init=False)

    def __post_init__(self):
        """Assemble the Nodes from a FlowModel."""
        # Create Nodes
        self.nodes = []
        for node_name in self.model.nodes.keys():
            # Create new node
            node = AsyncNode(node_name, self.model.nodes[node_name])
            self.nodes.append(node)


class LinkedFlowState(Enum):
    """Possible states for an actual flow."""

    UNUSED = auto()
    ACTIVE = auto()
    INACTIVE = auto()
    KILLED = auto()


class LinkedFlow(AsyncResource):
    """A Flow ready for execution. This is one-time use, not reentrant."""

    def __init__(self, flow: Flow):
        self.state: LinkedFlowState = LinkedFlowState.UNUSED
        self.flow = deepcopy(flow)  # We will be modifying this, so 'pass-by-value'
        self._stack = AsyncExitStack()
        # Create buffers
        (self._senders, self._receivers) = self._create_buffers(self.flow)
        # Hand off buffers
        for node in self.flow.nodes:
            node.offer_buffers(self._senders, self._receivers)

    @staticmethod
    def _create_buffers(
        flow: Flow,
    ) -> tuple[
        dict[SpecificPort, list[MemoryObjectSendStream]],
        dict[SpecificPort, MemoryObjectReceiveStream],
    ]:
        """Create buffers (send- and receive-streams)."""
        senders: dict[SpecificPort, list[MemoryObjectSendStream]] = defaultdict(list)
        receivers: dict[SpecificPort, MemoryObjectReceiveStream] = {}
        for edge_model in flow.model.edges:
            send_stream, receive_stream = create_memory_object_stream(
                max_buffer_size=20
            )  # TODO: add item_types
            senders[edge_model.start].append(send_stream)
            receivers[edge_model.end] = receive_stream
        return (senders, receivers)

    async def __aenter__(self):
        for node in self.flow.nodes:
            await self._stack.enter_async_context(node)
        self._state = LinkedFlowState.ACTIVE
        return self

    async def aclose(self):
        """Close a flow."""
        self._state = LinkedFlowState.KILLED
        await self._stack.aclose()


@dataclass
class FlowCancelScope:
    """Wraps (potentially) several different cancel scopes relating to a flow."""

    all_tasks: list[CancelScope]

    async def cancel_all(self):
        """Cancel all tasks."""
        for task in self.all_tasks:
            await task.cancel()


# TODO: The lifetime of all the tasks and the aenter/aexit status of the
# linked_flow should depend on the lifetime of this object?
@dataclass
class FlowHandle:
    """A Flow and its FlowCancelScope, with convenience methods."""

    linked_flow: LinkedFlow
    scopes: FlowCancelScope = field(init=False)
    _stack: AsyncExitStack = AsyncExitStack()

    async def stop_flow_immediately(self):
        """Stop all tasks and unwind stack."""
        await self.scopes.cancel_all()
        await self.linked_flow.aclose()

    @classmethod
    async def from_linked_flow(cls, linked_flow: LinkedFlow, tg: TaskGroup):
        """Create an instance from a LinkedFlow. The tasks start in the given TaskGroup."""
        self = cls(linked_flow)
        # aenter flow
        await self._stack.enter_async_context(linked_flow)
        # Start tasks
        scopes: list[CancelScope] = []
        for node in self.linked_flow.flow.nodes:
            # Start task
            cancel_scope: CancelScope = await tg.start(node)
            scopes.append(cancel_scope)

        flow_cancel_scope = FlowCancelScope(scopes)
        # Change status
        linked_flow.state = LinkedFlowState.ACTIVE
        self.scopes = flow_cancel_scope
        return self
