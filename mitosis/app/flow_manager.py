from collections import namedtuple
from contextlib import AsyncExitStack
from pathlib import Path
from typing import AsyncContextManager

from anyio import create_memory_object_stream, create_task_group
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..async_node import AsyncNode
from ..basics import FlowIntegrationException
from ..flow import Flow
from ..model import FlowModel, PersistentCellsModel, SpecificPort
from ..util import edge_matches_output_port

FlowHandle = namedtuple("FlowHandle", "flow cancelscope")


def add_to_attachments_stash(
    attachments: dict[SpecificPort, list[MemoryObjectSendStream]], flow: Flow
) -> None:
    """Add send_streams from the flow to the attachments."""
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
            if attachments.get(external_port) is None:
                attachments[external_port] = found_send_streams
            else:
                attachments[external_port] += found_send_streams


def remove_from_attachments_stash(
    attachments: dict[SpecificPort, list[MemoryObjectSendStream]], flow: Flow
) -> None:
    """Remove send_streams belonging to the flow from the attachments."""
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
            current_attachments = attachments[external_port]
            attachments[external_port] = [
                sender
                for sender in current_attachments
                if sender not in found_send_streams
            ]


class FlowManager(AsyncContextManager):
    """Manager for a set of runtime flows."""

    def __init__(self):
        self._stack = AsyncExitStack()
        self._tg = create_task_group()

        # A stash of senders, representing active connections from persistent cells to Flows.
        # Whenever a flow starts or stops, the persistent cells must update themselves from this.
        # This is NOT up to this manager but must be handled by whoever creates an instance of this class
        self._attachments: dict[SpecificPort, list[MemoryObjectSendStream]] = {}

    async def __aenter__(self):
        """Enter async context. Creates own taskgroup."""
        self._stack.enter_async_context(self._tg)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit async context. Will cancel all child tasks."""
        await self._tg.cancel_scope.cancel()
        await self._stack.__aexit__(None, None, None)

    def _create_flow(self, path: Path) -> Flow:
        """Create a Flow object, checking for proper connection with persistent cells."""
        flow_model = FlowModel.parse_file(path)
        return Flow(flow_model)

    async def start_flow(self, path: Path) -> FlowHandle:
        """Start a flow from a filepath."""
        # Create flow
        flow = self._create_flow(path)
        # Start flow
        cs = await run_flow_in_taskgroup(flow, self._tg)
        # Update stash of external connections.
        add_to_attachments_stash(self._attachments, flow)
        return FlowHandle(flow, cs)

    # TODO: Rethink this function. Should the order be:
    # Detach -> Update senders -> close flow
    # to avoid data loss?
    async def stop_flow(self, flow: Flow):
        """Detach a flow from persistent cells, then stop it."""
        # Detach Flow from external connections.
        remove_from_attachments_stash(self._attachments, flow)
        # Stop processes
        await flow.aclose()

    def get_attachments(self):
        """Return current set of attachments for the Persistent Cells."""
        return self._attachments
