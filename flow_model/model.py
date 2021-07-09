from typing import Optional
import logging

from enum import Enum, auto, unique
from pydantic import BaseModel, validator, root_validator

from networkx import DiGraph, NetworkXUnfeasible
from networkx.algorithms.dag import topological_sort


_LOGGER = logging.getLogger(__name__)

class FlowValidationException(Exception):
    pass

@unique
class NodeType(Enum):
    """Possible ways of supplying execute() functions"""
    PYTHON_FUNC = "python-func"
    INLINE_PYTHON = "inline-python"

class PortModel(BaseModel):
    """An input/output port on a node"""
    datatype: str

class SpecificPort(BaseModel):
    node: str
    port: str

    def __hash__(self):
        """Workaround to use this as dict keys"""
        return hash((type(self),) + (self.node, self.port))


class NodeModel(BaseModel):
    """A node in the flow graph"""
    source_type: NodeType
    source: str
    inputs: Optional[dict[str, PortModel]]
    outputs: Optional[dict[str, PortModel]]

class EdgeModel(BaseModel):
    """An edge between two ports"""
    start: SpecificPort
    end: SpecificPort
    active: bool = True

    def __hash__(self):
        """Workaround to use this as dict keys"""
        return hash((type(self),) + (self.start, self.end))

def digraph_from_edges(flow_edges: list[EdgeModel]):
    """Create DiGraph object from a list of edges"""
    G = DiGraph()
    # Create edges
    edges= []
    for edge in flow_edges:
        edge_tuple = (edge.start.node, edge.end.node)
        edges.append(edge_tuple)
    G.add_edges_from(edges)
    return G

class FlowModel(BaseModel):
    edges: list[EdgeModel]
    nodes: dict[str, NodeModel]

    @validator('edges')
    def no_cycles_present(cls, v):
        """From a DiGraph, we can easily check for cycles by trying to perform a topological sort"""
        dag = digraph_from_edges(v)
        try:
            topo_sorted = [a for a in topological_sort(dag)]
        except NetworkXUnfeasible as exc:
            raise FlowValidationException(f'The flow graph contains cycles') from exc
        return v

    @root_validator
    def nodes_must_exist(cls, values):
        """Check if edges reference existing nodes"""
        edges = values.get('edges')
        nodes = values.get('nodes')
        for edge in edges:
            if edge.start.node not in nodes:
                raise FlowValidationException(f'The node {edge.start} does not exist')
            if edge.end.node not in nodes:
                raise FlowValidationException(f'The node {edge.end} does not exist')
        return values
    
    @root_validator
    def match_number_of_inputs(cls, values):
        """Check if number of input ports match the number of input edges"""
        edges = values.get('edges')
        nodes = values.get('nodes')
        for node_name, node in nodes.items():
            num_input_edges = sum(1 for edge in edges if edge.end.node == node_name)
            num_input_types = 0 if node.inputs is None else len(node.inputs)
            if num_input_types != num_input_edges:
                raise FlowValidationException(f'The node {node_name} does not have the correct number of inputs ({num_input_types} but it should have {num_input_edges})')
        return values

    @root_validator
    def match_types_on_edges(cls, values):
        """For all edges, check that begin- and end-node has same type."""
        edges = values.get('edges')
        nodes = values.get('nodes')
        for edge in edges:
            if nodes[edge.start.node].outputs[edge.start.port].datatype != nodes[edge.end.node].inputs[edge.end.port].datatype:
                raise FlowValidationException(f'The input and output type on the edge {edge} does not match')
        return values

    def boot_order(self):
        """
        Returns the result of a topological sort of the underlying graph.
        This can fail if the graph contains cycles, but we check explicitly for this on construction
        """
        dag = digraph_from_edges(self.edges)
        return reversed([a for a in topological_sort(dag)])

    
            
    
