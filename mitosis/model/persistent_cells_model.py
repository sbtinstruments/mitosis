from pydantic import BaseModel, validator

from ..basics import PersistentCellValidationException
from . import NodeModel

class PersistentCellsModel(BaseModel):
    cells: dict[str, NodeModel]

    @validator('cells')
    def cells_have_no_inputs(v):
        for cell in v.values():
            if cell.inputs is not None:
                raise PersistentCellValidationException(f'Persistent cells can not have inputs.')
            return v

    @validator('cells')
    def cells_have_one_output(v):
        for cell in v.values():
            if len(cell.outputs.values()) != 1:
                raise PersistentCellValidationException(f'Persistent cells must have one output.')
            return v