from anyio import TASK_STATUS_IGNORED, create_task_group, run, create_memory_object_stream
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream

from pathlib import Path

import logging


from flow_model import FlowModel, EdgeModel, FlowValidationException
from async_node import AsyncNode


_LOGGER = logging.getLogger(__name__)


async def main():
    # Create Flow Model
    G = FlowModel.parse_file(Path('mygraph/mygraph.json'))
    boot_order = G.boot_order()
    # Create buffers
    senders: dict[EdgeModel, MemoryObjectSendStream] = {}
    receivers: dict[EdgeModel, MemoryObjectReceiveStream] = {}
    for edgemodel in G.edges:
        send_stream, receive_stream = create_memory_object_stream(max_buffer_size=20) #TODO: add item_types
        senders[edgemodel] = send_stream
        receivers[edgemodel] = receive_stream

    
    # Spawn Tasks
    async with create_task_group() as task_group:
        for node_name in boot_order:
            # Create new node
            node = AsyncNode(node_name, G.nodes[node_name], senders, receivers)
            # Start task
            await task_group.start(node)

run(main)

