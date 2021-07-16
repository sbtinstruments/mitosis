from dataclasses import dataclass
from pathlib import Path
from typing import AsyncContextManager

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

from mitosis.model.edge_model import SpecificPort

from .async_node import AsyncNode
from .model import EdgeModel, FlowModel, PersistentCellsModel
from .util import edge_matches_output_port


class Flow(AsyncResource):
    """A set of tasks, representing a flow in an app."""

    _scopes: dict[str, CancelScope]

    async def shut_down(self):
        for cs in self._scopes.values():
            cs.cancel()

    async def aclose(self):
        pass


class MitosisApp(AsyncContextManager):
    """Main application for handling a set of persistent nodes and a set of runtime flows."""

    def __init__(self, tg: TaskGroup, persistent_cells_path: Path):
        self._tg = tg
        self.persistent_cells_model = PersistentCellsModel.parse_file(
            persistent_cells_path
        )
        self.flows: dict[str, TaskGroup]
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
        for cell_name, cell_model in self.persistent_cells_model.cells.items():
            cell = AsyncNode(cell_name, cell_model, {}, {}, self._attachments_receivers)
            await self._tg.start(cell)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit async context."""
        pass

    async def spawn_flow(self, tg: TaskGroup, path: Path):
        """Spawn a new Flow in the app. Attaches to persistent cells as needed."""

        async def _flow() -> dict[str, CancelScope]:
            # Create Flow Model
            flow_model = FlowModel.parse_file(path)
            # boot_order = flow_model.boot_order()

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
            scopes: dict[str, CancelScope] = {}
            for node_name, node_model in flow_model.nodes.items():
                # Create new node
                node = AsyncNode(node_name, node_model, senders, receivers)
                # Start task
                scopes[node_name] = await tg.start(node)

            # Attach Flow to external connections.
            if flow_model.externals is not None:
                for external_port in flow_model.externals.connections:
                    # Find all connections to that external port
                    found_send_streams = [
                        send_stream
                        for edge_model, send_stream in senders.items()
                        if edge_matches_output_port(
                            external_port.node, external_port.port, edge_model
                        )
                    ]
                    self._attachments[external_port] = found_send_streams

            return scopes

        # Create flow
        scopes = await _flow()
        # Inform persistent cells that new attachments may be available
        for sender in self._attachments_senders:
            await sender.send(self._attachments)

        return FlowHandle(scopes)
