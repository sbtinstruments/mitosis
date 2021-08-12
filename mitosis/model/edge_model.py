from pydantic import BaseModel


class SpecificPort(BaseModel, frozen=True):
    node: str
    port: str


class EdgeModel(BaseModel, frozen=True):
    """An edge between two ports"""

    start: SpecificPort
    end: SpecificPort
    active: bool = True
