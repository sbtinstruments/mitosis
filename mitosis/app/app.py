import logging
from contextlib import AsyncExitStack
from pathlib import Path
from typing import AsyncContextManager, Optional

from anyio import create_memory_object_stream, create_task_group
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..async_node import AsyncNode
from ..basics import FlowIntegrationException
from ..flow import Flow, FlowCancelScope, run_flow_in_taskgroup
from ..model import FlowModel, PersistentCellsModel, SpecificPort
from ..util import edge_matches_output_port
from .flow_manager import FlowManager
from .persistent_cell_manager import PersistentCellManager

_LOGGER = logging.getLogger(__name__)

from collections import namedtuple

FlowHandle = namedtuple("FlowHandle", "flow cancelscope")


class MitosisApp(AsyncContextManager):
    """Main application for handling a set of persistent nodes and a set of runtime flows."""

    def __init__(self, persistent_cells_path: Path):
        self._stack = AsyncExitStack()
        self._tg = create_task_group()
        self._pcman = PersistentCellManager(persistent_cells_path, self._tg)
        self._fman = FlowManager()

        self.running_flows: dict[Path, FlowHandle] = {}

        # TODO: Is this still needed??
        # A global stash of senders, representing active connections from persistent cells to Flows.
        # Whenever a flow starts or stops, the persistent cells must update themselves from this
        self._attachments: dict[SpecificPort, list[MemoryObjectSendStream]] = {}

    async def __aenter__(self):
        """Enter async context."""
        await self._stack.enter_async_context(self._tg)
        # Starts the persistent cells
        await self._stack.enter_async_context(self._pcman)
        await self._stack.enter_async_context(self._fman)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit async context."""
        await self._stack.__aexit__(None, None, None)

    async def start_flow(self, path: Path):
        """
        Start a flow in the app on a given AsyncExitStack. Attaches to persistent cells as needed.
        """
        # Start flow
        fh = await self._fman.start_flow(path)
        # Register flow
        self.running_flows[path] = fh
        # Inform persistent cells that new attachments may be available
        await self._pcman.update_attachments(self._fman.get_attachments())

    # TODO: Refactor to use managers
    async def stop_flow(self, path: Path):
        """Detach a flow from persistent cells, then stop it."""
        if self.running_flows.get(path) is None:
            _LOGGER.warning(
                f"The flow at {path} cannot be stopped, since it is not running"
            )
            return

        # Get flow handle
        fh = self.running_flows[path]
        flow = fh.flow
        # Detach Flow from external connections
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
        await self.update_attachments()
        # Stop processes
        await flow.aclose()
