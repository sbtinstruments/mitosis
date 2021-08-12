from typing import List, Set, Union

from ..model import EdgeModel


def without_keys(d: dict, keys: Union[List[str], Set[str]]):
    return {x: d[x] for x in d if x not in keys}


def edge_matches_input_port(node_name, port_name, edge_model: EdgeModel):
    nodes_match = node_name == edge_model.end.node
    ports_match = port_name == edge_model.end.port
    return nodes_match and ports_match


def edge_matches_output_port(node_name, port_name, edge_model: EdgeModel):
    nodes_match = node_name == edge_model.start.node
    ports_match = port_name == edge_model.start.port
    return nodes_match and ports_match
