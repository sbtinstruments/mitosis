from dataclasses import dataclass
from pathlib import Path
from typing import AsyncContextManager

from anyio import (
    TASK_STATUS_IGNORED,
    CancelScope,
    create_memory_object_stream,
    create_task_group,
    run,
    sleep,
)
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from .async_node import AsyncNode
from .model import EdgeModel, FlowModel, PersistentCellsModel


@dataclass
class FlowHandle:
    """Wrapper for an AnyIO TaskGroup, representing a flow"""

    _cs: CancelScope

    async def shut_down(self):
        print("Cancelling!")
        self._cs.cancel()


class MitosisApp(AsyncContextManager):
    """Main application for handling a set of persistent nodes and a set of runtime flows."""

    def __init__(self, tg: TaskGroup, persistent_cells_path: Path):
        self._tg = tg
        self.persistent_cells_model = PersistentCellsModel.parse_file(
            persistent_cells_path
        )
        self.flows: dict[str, TaskGroup]

    async def __aenter__(self):
        """Enter async context."""
        for cell_name, cell_model in self.persistent_cells_model.cells.items():
            cell = AsyncNode(cell_name, cell_model, {}, {})
            await self._tg.start(cell)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit async context."""
        pass

    async def spawn_flow(self, tg: TaskGroup, path: Path):
        """Spawn a new Flow in the app. Attaches to persistent cells as needed."""

        async def _flow():
            # Create Flow Model
            flow_model = FlowModel.parse_file(path)
            boot_order = flow_model.boot_order()
            # Create buffers
            senders: dict[EdgeModel, MemoryObjectSendStream] = {}
            receivers: dict[EdgeModel, MemoryObjectReceiveStream] = {}
            for edge_model in flow_model.edges:
                send_stream, receive_stream = create_memory_object_stream(
                    max_buffer_size=20
                )  # TODO: add item_types
                senders[edge_model] = send_stream
                receivers[edge_model] = receive_stream

            # Spawn Tasks
            async with CancelScope() as scope:
                for node_name in boot_order:
                    # Create new node
                    node = AsyncNode(
                        node_name, flow_model.nodes[node_name], senders, receivers
                    )
                    # Start task
                    await tg.start(node)

            return scope

        flow_scope = await _flow()
        return FlowHandle(flow_scope)
