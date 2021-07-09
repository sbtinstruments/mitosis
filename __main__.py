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

# async def start_producer_node(out: float, child: AveragerNode, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
#     async with ProducerNode(out) as node:
#         task_status.started()
#         node.add_child(child)
#         await node.run_method()

# async def start_averager_node(out: float, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
#     async with AveragerNode(out) as node:
#         task_status.started(node)
#         await node.run_method()


# async def main():
#     async with create_task_group() as task_group:
#         averager_node = await task_group.start(start_averager_node, 0.5)
#         await task_group.start(start_producer_node, 3, averager_node)

# run(main)

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

# spawn tasks
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
            
            

            await task_group.start(node)

run(main)
