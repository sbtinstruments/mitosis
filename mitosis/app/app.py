import logging
from contextlib import AsyncExitStack
from pathlib import Path
from typing import AsyncContextManager, Optional

from anyio import create_memory_object_stream, create_task_group
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..async_node import AsyncNode
from ..basics import FlowIntegrationException
from ..flow import FlowHandle
from ..model import FlowModel, PersistentCellsModel, SpecificPort
from ..util import edge_matches_output_port
from .flow_manager import FlowManager
from .persistent_cell_manager import PersistentCellManager

_LOGGER = logging.getLogger(__name__)


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

    async def start_flow(self, path: Path, key=None):
        """
        Start a flow in the app on a given AsyncExitStack. Attaches to persistent cells as needed.
        """
        # Start flow
        flow_handle = await self._fman.start_flow(path)
        # Inform persistent cells that new attachments may be available
        await self._pcman.update_attachments(self._fman.get_attachments())
        # Register flow
        flow_key = path if key is None else key
        self.running_flows[flow_key] = flow_handle

    async def stop_flow(self, key):
        """Stop a flow and detach it from persistent cells."""
        if self.running_flows.get(key) is None:
            _LOGGER.warning(
                f"The flow at {key} cannot be stopped, since it is not running"
            )
            return

        # Get flow handle
        flow_handle = self.running_flows[key]
        flow = flow_handle.flow
        # Stop flow
        await self._fman.stop_flow(flow)
        # Inform persistent cells that attachments stash may have changed
        await self._pcman.update_attachments(self._fman.get_attachments())
        # Deregister flow
        del self.running_flows[key]
