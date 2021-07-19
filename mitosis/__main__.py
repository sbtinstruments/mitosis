from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncContextManager, Optional

from anyio import (
    TASK_STATUS_IGNORED,
    CancelScope,
    create_memory_object_stream,
    create_task_group,
    move_on_after,
    run,
    sleep,
)
from anyio.abc import AsyncResource, TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from .async_node import AsyncNode
from .basics import FlowIntegrationException
from .model import EdgeModel, FlowModel, PersistentCellsModel, SpecificPort
from .util import edge_matches_output_port


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
        self._tg = create_task_group()

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

        await self._stack.enter_async_context(self._tg)
        for node in nodes:
            # Start task
            await self._tg.start(node)

        return self

    async def aclose(self):
        """Close a flow. Stop all computation."""
        await self._tg.cancel_scope.cancel()
        await self._stack.__aexit__(None, None, None)


class MitosisApp(AsyncContextManager):
    """Main application for handling a set of persistent nodes and a set of runtime flows."""

    def __init__(self, tg: TaskGroup, persistent_cells_path: Path):
        self._tg = tg
        self._stack = AsyncExitStack()
        self.persistent_cells_model = PersistentCellsModel.parse_file(
            persistent_cells_path
        )
        # A global stash of senders, representing active connections from persistent cells to Flows.
        # Whenever a flow starts or stops, the persistent cells must update themselves from this
        self._attachments: dict[SpecificPort, list[MemoryObjectSendStream]] = {}
        # These streams communicate changes in attachments to the persistent cells
        self._attachments_senders: list[MemoryObjectSendStream] = []
        self._attachments_receivers: dict[str, MemoryObjectReceiveStream] = {}
        for cell_name in self.persistent_cells_model.cells.keys():
            send_stream, receive_stream = create_memory_object_stream(max_buffer_size=1)
            self._attachments_senders.append(send_stream)
            self._attachments_receivers[cell_name] = receive_stream

    async def __aenter__(self):
        """Enter async context."""
        # Create persistent cells
        for cell_name, cell_model in self.persistent_cells_model.cells.items():
            cell = AsyncNode(cell_name, cell_model, {}, {}, self._attachments_receivers)
            await self._stack.enter_async_context(cell)
            await self._tg.start(cell)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit async context."""
        await self._tg.cancel_scope.cancel()
        await self._stack.__aexit__(None, None, None)

    def create_flow(self, path: Path) -> Flow:
        """Create a Flow object, checking for proper connection with persistent cells"""
        # Create Flow Model
        flow_model = FlowModel.parse_file(path)
        # Check if FlowModel fits into the App
        if not self.persistent_cells_model.is_subset(flow_model.externals):
            raise FlowIntegrationException(
                "This flow's external connections does not fit with the persistent cells."
            )
        return Flow(flow_model)

    async def update_attachments(self):
        """Inform persistent cells that new attachments may be available."""
        for sender in self._attachments_senders:
            await sender.send(self._attachments)

    async def start_flow(self, flow: Flow):
        """
        Start a flow in the app on a given AsyncExitStack. Attaches to persistent cells as needed.
        This flow must already be checked to fit in the app.
        """
        # Start flow
        await self._stack.enter_async_context(flow)
        # Attach Flow to external connections.
        if flow._model.externals is not None:
            for external_port in flow._model.externals.connections:
                # Find all connections to that external port
                found_send_streams = [
                    send_stream
                    for edge_model, send_stream in flow._senders.items()
                    if edge_matches_output_port(
                        external_port.node, external_port.port, edge_model
                    )
                ]
                if self._attachments.get(external_port) is None:
                    self._attachments[external_port] = found_send_streams
                else:
                    self._attachments[external_port] += found_send_streams
        # Inform persistent cells that new attachments may be available
        print(self._attachments[SpecificPort(node="Ones", port="IntOut")])
        await self.update_attachments()

    async def stop_flow(self, flow: Flow):
        """Detach a flow from persistent cells, then stop it"""
        # Detach Flow from external connections.
        if flow._model.externals is not None:
            for external_port in flow._model.externals.connections:
                # Find all connections to that external port
                found_send_streams = [
                    send_stream
                    for edge_model, send_stream in flow._senders.items()
                    if edge_matches_output_port(
                        external_port.node, external_port.port, edge_model
                    )
                ]
                current_attachments = self._attachments[external_port]
                self._attachments[external_port] = [
                    sender
                    for sender in current_attachments
                    if sender not in found_send_streams
                ]
        print(self._attachments[SpecificPort(node="Ones", port="IntOut")])
        await self.update_attachments()
        # Stop processes
        await flow.aclose()
