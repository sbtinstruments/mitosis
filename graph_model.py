from typing import Optional
from enum import Enum, auto, unique
from pydantic import BaseModel

@unique
class NodeType(Enum):
    """Possible ways of supplying execute() functions"""
    PYTHONFUNC = "python-func"
    INLINEPYTHON = "inline-python"

class Edge(BaseModel):
    start: str
    end: str

class Node(BaseModel):
    source_type: NodeType
    source: str
    frequency: float
    input_types: Optional[list[str]]
    output_type: Optional[str]

class GraphModel(BaseModel):
    edges: list[Edge]
    nodes: dict[str, Node]
