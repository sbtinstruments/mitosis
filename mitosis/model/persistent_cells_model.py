from typing import Optional

from pydantic import BaseModel, validator

from ..basics import PersistentCellValidationException
from . import NodeModel


class PersistentCellsModel(BaseModel):
    cells: dict[str, NodeModel]

    @validator("cells")
    def cells_have_no_inputs(v):
        for cell in v.values():
            if cell.inputs is not None:
                raise PersistentCellValidationException(
                    "Persistent cells can not have inputs."
                )
            return v

    @validator("cells")
    def cells_have_one_output(v):
        for cell in v.values():
            if len(cell.outputs.values()) != 1:
                raise PersistentCellValidationException(
                    "Persistent cells must have one output."
                )
            return v

    def is_subset(self, attachments):
        """Determine if all external ports are supplied by this set of persistent cells."""
        # TODO: change this from checking external_ports to attachments
        if external_ports is not None:
            for external_connection in external_ports.connections:
                # Find all connections to that external port
                try:
                    cell = self.cells[external_connection.node]
                except KeyError:
                    return False
                if cell.outputs is None:
                    return False
                try:
                    cell.outputs[external_connection.port]
                except KeyError:
                    return False
        return True
