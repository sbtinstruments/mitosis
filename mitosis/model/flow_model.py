import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Optional, Type, TypeVar

from networkx import DiGraph, NetworkXUnfeasible
from networkx.algorithms.dag import topological_sort
from pydantic import BaseModel, root_validator, validator

from ..basics import FlowValidationException
from . import EdgeModel, ExternalPortsModel, NodeModel

_LOGGER = logging.getLogger(__name__)


def digraph_from_edges(flow_edges: list[EdgeModel]):
    """Create DiGraph object from a list of edges."""
    G = DiGraph()
    # Create edges
    edges = []
    for edge in flow_edges:
        edge_tuple = (edge.start.node, edge.end.node)
        edges.append(edge_tuple)
    G.add_edges_from(edges)
    return G


class FlowModel(BaseModel):
    edges: list[EdgeModel]
    nodes: dict[str, NodeModel]
    # Optional list of needed external port. These should be available in the persistent cells of the app
    externals: Optional[ExternalPortsModel]

    # Checks:
    # Edges should go between existing ports on existing nodes :: DONE
    # Input ports should have exactly one edge connected to it
    # Output ports should have at least one edge connected to it
    # For now, we don't allow cycles in the graph :: DONE

    @validator("edges")
    def no_cycles_present(v):
        """From a DiGraph, we can easily check for cycles by trying to perform a topological sort"""
        dag = digraph_from_edges(v)
        try:
            topo_sorted = [a for a in topological_sort(dag)]
        except NetworkXUnfeasible as exc:
            raise FlowValidationException(f"The flow graph contains cycles") from exc
        return v

    @root_validator
    def ports_must_exist(cls, values):
        """Check if edges reference existing ports."""
        edges = values.get("edges")
        nodes = values.get("nodes")
        externals = values.get("externals")
        external_ports = externals.connections
        for edge in edges:
            # Start
            if edge.start.node in nodes:
                if not edge.start.port in nodes[edge.start.node].outputs:
                    raise FlowValidationException(
                        f"The port {edge.start.port} on the node {edge.start.node} does not exist"
                    )
            else:
                # The node wasn't found in the FlowGraph. It might still be in the list of external nodes
                for ext_port in external_ports:
                    if edge.start.node == ext_port.node:
                        # Node found. Check port
                        if not edge.start.port == ext_port.port:
                            raise FlowValidationException(
                                f"The port {edge.start.port} on the node {edge.start.node} does not exist"
                            )
                        break
                else:
                    raise FlowValidationException(
                        f"The node {edge.start.node} does not exist"
                    )
            # End
            if edge.end.node in nodes:
                if not edge.end.port in nodes[edge.end.node].inputs:
                    raise FlowValidationException(
                        f"The port {edge.end.port} on the node {edge.end.node} does not exist"
                    )
            else:
                raise FlowValidationException(
                    f"The node {edge.end.node} does not exist"
                )
        return values

    @root_validator
    def match_number_of_inputs(cls, values):
        """
        Check if number of input ports match the number of input edges.
        """
        edges = values.get("edges")
        nodes = values.get("nodes")
        for node_name, node in nodes.items():
            num_input_edges = sum(1 for edge in edges if edge.end.node == node_name)
            num_input_types = 0 if node.inputs is None else len(node.inputs)
            if num_input_types != num_input_edges:
                raise FlowValidationException(
                    f"The node {node_name} does not have the correct number of inputs ({num_input_types} but it should have {num_input_edges})"
                )
        return values

    # No type-checking at the moment
    # @root_validator
    # def match_types_on_edges(cls, values):
    #     """For all edges, check that begin- and end-node has same type."""
    #     edges = values.get("edges")
    #     nodes = values.get("nodes")
    #     for edge in edges:
    #         if (
    #             nodes[edge.start.node].outputs[edge.start.port].datatype
    #             != nodes[edge.end.node].inputs[edge.end.port].datatype
    #         ):
    #             raise FlowValidationException(
    #                 f"The input and output type on the edge {edge} does not match"
    #             )
    #     return values

    # Deprecated? Boot order should not matter anymore
    def boot_order(self):
        """
        Returns the result of a topological sort of the underlying graph.
        This can fail if the graph contains cycles, but we check explicitly for this on construction
        """
        dag = digraph_from_edges(self.edges)
        return reversed([a for a in topological_sort(dag)])
