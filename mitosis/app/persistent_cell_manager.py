from contextlib import AsyncExitStack
from pathlib import Path

from anyio import create_memory_object_stream
from anyio.abc import AsyncResource, TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..async_node import AsyncNode
from ..basics import FlowIntegrationException
from ..model import PersistentCellsModel, SpecificPort


class PersistentCellManager(AsyncResource):
    """Manager for a set of persistent nodes."""

    def __init__(self, persistent_cells_path: Path, tg: TaskGroup):
        """Create an instance of the manager. A path must be supplied at init time."""
        self.persistent_cells_model = PersistentCellsModel.parse_file(
            persistent_cells_path
        )
        self._tg = tg
        self._stack = AsyncExitStack()

        # These streams communicate changes in attachments to the persistent cells
        self._attachments_senders: list[MemoryObjectSendStream] = []
        self._attachments_receivers: dict[str, MemoryObjectReceiveStream] = {}
        for cell_name in self.persistent_cells_model.cells.keys():
            send_stream, receive_stream = create_memory_object_stream(max_buffer_size=1)
            self._attachments_senders.append(send_stream)
            self._attachments_receivers[cell_name] = receive_stream

    async def __aenter__(self):
        """Enter async context. This starts the underlying processes."""
        # Create persistent cells
        for cell_name, cell_model in self.persistent_cells_model.cells.items():
            cell = AsyncNode(cell_name, cell_model, {}, {}, self._attachments_receivers)
            await self._stack.enter_async_context(cell)
            await self._tg.start(cell)
        return self

    async def aclose(self):
        """Exit async context."""
        await self._tg.cancel_scope.cancel()
        await self._stack.aclose()

    async def update_attachments(
        self, attachments: dict[SpecificPort, list[MemoryObjectSendStream]]
    ):
        """Inform persistent cells that new attachments may be available."""
        # Check for any non-existent requests. This means a flow has been created which doesn't fit
        if not self.persistent_cells_model.is_subset(attachments):
            raise FlowIntegrationException(
                "This flow's external connections does not fit with the persistent cells."
            )
        for sender in self._attachments_senders:
            await sender.send(attachments)
