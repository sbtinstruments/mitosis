from pydantic import BaseModel


# Does `frozen=True` work? :) I've never seen it before.
# I've always used:
#
# class Config:
#     frozen = True
#
class SpecificPort(BaseModel, frozen=True):
    node: str
    port: str


class EdgeModel(BaseModel, frozen=True):
    """An edge between two ports"""

    start: SpecificPort
    end: SpecificPort
    active: bool = True
