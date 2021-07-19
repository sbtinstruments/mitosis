from pydantic import BaseModel, validator

from mitosis.basics.exceptions import FlowValidationException

from ..basics import FlowValidationException
from . import SpecificPort


class ExternalPortsModel(BaseModel):
    connections: list[SpecificPort]

    # Currently, we only allow each persistent cell to have one output.
    # Consequently, we can't allow each node to appear in the list of ports more than once.
    @validator("connections")
    def all_nodes_are_unique(v):
        nodes_as_set = set(sp.node for sp in v)
        if len(nodes_as_set) != len(v):
            raise FlowValidationException(
                f"A node can only be referenced once as an external connection"
            )
        return v
