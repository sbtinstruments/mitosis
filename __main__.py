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

from flow_model import FlowModel, FlowValidationException

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



def validate_flow_model(G: FlowModel):
    """Validates a Graph Model to our custom specs"""
    # Check if edges reference existing nodes
    for edge in G.edges:
        if edge.start.node not in G.nodes:
            _LOGGER.error(f'The node {edge.start} does not exist')
            return False
        if edge.end.node not in G.nodes:
            _LOGGER.error(f'The node {edge.end} does not exist')
            return False
    # Check that number of inputs match number of edges going in
    for node_name, node in G.nodes.items():
        num_input_edges = sum(1 for edge in G.edges if edge.end.node == node_name)
        num_input_types = 0 if node.inputs is None else len(node.inputs)
        if num_input_types != num_input_edges:
            _LOGGER.error(f'The node {node_name} does not have the correct number of inputs ({num_input_types} but it should have {num_input_edges})')
            return False

    # For all edges, check that begin- and end-node has same type.
    for edge in G.edges:
        if G.nodes[edge.start.node].outputs[edge.start.port].datatype != G.nodes[edge.end.node].inputs[edge.end.port].datatype:
            _LOGGER.error(f'The input and output type on the edge {edge} does not match')
            return False
    
    # TODO: typecheck source/execute()-functions

    return True


def boot_order(model: FlowModel):
    """
    This returns a topological sort of nodes, which gives us the correct 
    order to spin up nodes. An error in this sorting also detects cycles
    """
    # Create DiGraph object
    G = nx.DiGraph()
    # Create edges
    edges= []
    for edge in model.edges:
        edge_tuple = (edge.start.node, edge.end.node)
        edges.append(edge_tuple)
    G.add_edges_from(edges)
    try:
        topo_sorted = [a for a in topological_sort(G)]
    except nx.NetworkXUnfeasible as exc:
        _LOGGER.error(f'The flow graph contains cycles')
        raise FlowValidationException from exc
    return reversed([a for a in topological_sort(G)])


def create_flow_model(graph_file: Path):
    """This creates a FlowModel from a json file"""
    # Create model
    model = FlowModel.parse_file(graph_file)
    # Validate model
    if not validate_flow_model(model):
        _LOGGER.error(f'Flow model not valid')
        return None
    return model


# spawn tasks
async def main():
    # Create Flow Model
    G = FlowModel.parse_file(Path('mygraph.json'))
    boot_order = G.boot_order()
    # Spawn Tasks
    async with create_task_group() as task_group:
        for node_name in boot_order:
            node = AsyncNode(G.nodes[node_name])
            averager_node = await task_group.start(node, 0.5)
            await task_group.start(start_producer_node, 3, averager_node)

run(main)
