from anyio.streams.memory import MemoryObjectSendStream
from pydantic import BaseModel, validator

from ..basics import PersistentCellValidationException
from . import NodeModel, SpecificPort


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

    def is_subset(self, attachments: dict[SpecificPort, list[MemoryObjectSendStream]]):
        """Determine if all requested attachments are supplied by this set of persistent cells."""
        if attachments is not None:
            for specific_port in attachments.keys():
                # Find all connections to that external port
                try:
                    cell = self.cells[specific_port.node]
                except KeyError:
                    return False
                if cell.outputs is None:
                    return False
                try:
                    cell.outputs[specific_port.port]
                except KeyError:
                    return False
        return True
