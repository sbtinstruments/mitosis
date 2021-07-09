from anyio import TASK_STATUS_IGNORED, create_task_group, run
from anyio.abc import TaskStatus

#from async_node import ProducerNode, AveragerNode

from pathlib import Path
import networkx as nx
import json

import logging
from networkx.algorithms.operators.unary import reverse

from networkx.algorithms.shortest_paths.unweighted import predecessor
from networkx.algorithms.dag import topological_sort
import util

from flow_model import FlowModel, EdgeModel, FlowValidationException
from async_node import AsyncNode

from queue import Queue

_LOGGER = logging.getLogger(__name__)

## TODO: Find a more async-friendly queue, perhaps anyio.MemoryObjectStream? Something that can be await'ed.
## TODO: Perhaps make all ports up front, would maybe make initialization more readable? Would also allow for cycles - if anyone wants to use that

def match_input_ports_to_edges(G: FlowModel, node: AsyncNode):
    res: dict[EdgeModel, Queue] = {}
    for buffer_name, buffer in node.ins.buffers.items():
        # Find an edge which ends in the same node and port
        found_edge = None
        for edge in G.edges:
            edge_end_node_model = G.nodes[edge.end.node]
            nodes_match = edge_end_node_model == node.model
            ports_match = edge.end.port == buffer_name
            if nodes_match and ports_match:
                found_edge = edge
        if found_edge is None:
            raise FlowValidationException(f'Edge could not be matched to buffer')
        res[found_edge] = buffer
    return res



async def main():
    # Create Flow Model
    G = FlowModel.parse_file(Path('mygraph.json'))
    boot_order = G.boot_order()
    
    # Spawn Tasks
    buffers: dict[EdgeModel, Queue] = {}
    async with create_task_group() as task_group:
        for node_name in boot_order:
            # Create new node
            node = AsyncNode(node_name, G.nodes[node_name], buffers)
            # Update dict of available buffers
            input_edges = match_input_ports_to_edges(G, node)
            buffers.update(input_edges)
            # Start task
            await task_group.start(node)

run(main)

