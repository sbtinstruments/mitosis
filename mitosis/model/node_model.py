from enum import Enum, auto, unique
from typing import Any, Callable, Optional

from pydantic import BaseModel, root_validator, validator

from ..basics import FlowValidationException

# The name of the main function in a node
_EXECUTABLE_NAME = "main"


@unique
class ExecutableType(str, Enum):
    """Possible ways of supplying execute() functions"""

    PYTHON_FUNC = "python-func"
    INLINE_PYTHON = "inline-python"


class ExecutableModel(BaseModel):
    type: ExecutableType
    source: str
    code: dict[str, Any]

    @root_validator(pre=True)
    def load_function(cls, values):
        type_ = values.get("type")
        source = values.get("source")
        if type_ == ExecutableType.PYTHON_FUNC:
            # Read file
            with open(source) as f:
                source_str = f.read()
            code_object = compile(source_str, source, "exec")
            globals_ = {}
            exec(code_object, globals_)
            values["code"] = globals_
        else:
            raise FlowValidationException(f"Unsupported Executable type")
        return values

    def get_executable(self):
        """Return code to run every cycle."""
        return self.code[_EXECUTABLE_NAME]

    # TODO: validate that we have a main function in self.code


class PortModel(BaseModel):
    """An input/output port on a node."""

    datatype: str


class NodeConfig(BaseModel):
    """A configuration object for a node. This can be a node from a flow graph or a persistent node."""

    # If true, a node will not run when it has no active connections in its output ports
    shut_down_when_ignored = False
    # Speed
    frequency: float

    @validator("frequency")
    def frequency_must_be_positive(v):
        if v <= 0.0:
            raise FlowValidationException(f"Node frequency must be positive")
        return v


class NodeModel(BaseModel):
    """
    A node in the flow graph.

    This can be a node from a flow graph or a persistent node.
    """

    executable: ExecutableModel
    config: NodeConfig
    inputs: Optional[dict[str, PortModel]]
    outputs: Optional[dict[str, PortModel]]

    def get_executable(self):
        """Return code to run every cycle."""
        return self.executable.get_executable()
