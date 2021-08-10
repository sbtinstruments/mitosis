import logging
from contextlib import AsyncExitStack
from pathlib import Path
from typing import AsyncContextManager, Optional

from anyio import create_memory_object_stream, create_task_group
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..async_node import AsyncNode
from ..basics import FlowIntegrationException, KeyNotUniqueException
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

        # TODO: Is this still needed??
        # A global stash of senders, representing active connections from persistent cells to Flows.
        # Whenever a flow starts or stops, the persistent cells must update themselves from this
        self._attachments: dict[SpecificPort, list[MemoryObjectSendStream]] = {}

    async def __aenter__(self):
        """Enter async context."""
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(self._tg)
            # Starts the persistent cells
            await stack.enter_async_context(self._pcman)
            await stack.enter_async_context(self._fman)
            # Transfer ownership to instance
            self._stack = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit async context."""
        # TODO: Forward the exceptions to the stack's `__aexit__`. Otherwise, you effectively
        # hide the exceptions from the underlying task group.
        await self._stack.__aexit__(None, None, None)

    async def start_flow(self, path: Path, key=None):
        """
        Start a flow in the app on a given AsyncExitStack. Attaches to persistent cells as needed.
        """
        # Start flow
        try:
            await self._fman.start_flow(path, key)
        # TODO: Let the exception propagate. Otherwise, how will the user know that something
        # went wrong?
        #
        # Also, let the caller do the logging. Aka "silent per default" principle.
        # Alternatively, use a much lower log level. E.g. "debug".
        except KeyNotUniqueException as exc:
            _LOGGER.error(f"The key '{exc.key}' is already in use.")
            return
        # Inform persistent cells that new attachments may be available
        await self._pcman.update_attachments(self._fman.get_attachments())

    async def stop_flow(self, key):
        """Stop a flow and detach it from persistent cells."""
        # Stop flow
        await self._fman.stop_flow(key)
        # Inform persistent cells that attachments stash may have changed
        await self._pcman.update_attachments(self._fman.get_attachments())
