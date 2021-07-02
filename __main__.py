#from anyio import TASK_STATUS_IGNORED, create_task_group, run
#from anyio.abc import TaskStatus

#from async_node import ProducerNode, AveragerNode

from pathlib import Path
import networkx as nx
import json

import logging

from networkx.algorithms.shortest_paths.unweighted import predecessor
import util

from graph_model import GraphModel, NodeType, Edge

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

def edge_comparator(a: Edge, b: Edge):
    """This can be used to sort edges lexicographically by their nodes"""

def validate_graph_model(G: GraphModel):
    """Validates a Graph Model to our custom specs"""
    # Check if edges reference existing nodes
    for edge in G.edges:
        if edge.start not in G.nodes:
            _LOGGER.error(f'The node {edge.start} does not exist')
            return False
        if edge.end not in G.nodes:
            _LOGGER.error(f'The node {edge.end} does not exist')
            return False
    # Check that number of inputs match number of edges going in
    for node_name, node in G.nodes.items():
        num_input_edges = sum(1 for edge in G.edges if edge.end == node_name)
        num_input_types = 0 if node.input_types is None else len(node.input_types)
        if num_input_types != num_input_edges:
            _LOGGER.error(f'The node {node_name} does not have the correct number of inputs ({num_input_types} but it should have {num_input_edges})')
            return False
    # For all inputs, check that the origin has the same type
    for node_name, node in G.nodes.items():
        if node.input_types is not None:
            # Order input edges lexicographically
            input_edges = sorted([e for e in G.edges if e.end == node_name], key=lambda e:e.start)
            assert(len(input_edges) == len(node.input_types))
            for input_type, input_edge in zip(node.input_types, input_edges):
                if input_type != G.nodes[input_edge.start].output_type:
                    predecessor_name = input_edge.start
                    predecessor = G.nodes[predecessor_name]
                    output_type = predecessor.output_type
                    _LOGGER.error(f'Output type ({output_type}) in node {predecessor_name} does not match input type ({input_type}) in node {node_name}')
                    return False
    return True


def create_graph(graph_file: Path):
    """This creates a DiGraph object from a graph.json file"""
    # Create model
    model = GraphModel.parse_file(graph_file)
    # Validate model
    if not validate_graph_model(model):
        _LOGGER.error(f'Graph model not valid')
        return None
    # Create DiGraph object
    G = nx.DiGraph()
    # Create edges
    edges= []
    for edge in model.edges:
        edge_tuple = (edge.start, edge.end)
        edges.append(edge_tuple)
    G.add_edges_from(edges)
    # Add metadata to existing nodes
    node_data = model.nodes
    for node in G.nodes:
        G.add_node(node, data=node_data[node])
    return G


G = create_graph(Path('mygraph.json'))
print(G.nodes)
# G = create_graph(Path('mygraph.json'))
# graph_valid = validate_graph(G)

# print(list(G.predecessors('D')))
# print(G.nodes['A'])