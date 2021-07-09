from flow_model.model import FlowModel
from typing import Optional
from anyio import TASK_STATUS_IGNORED
from anyio.abc import TaskStatus

from queue import Queue

from flow_model import NodeModel, PortModel, EdgeModel

class InGroup:
    """Contains the node's input buffers"""
    def __init__(self, inputs: Optional[dict[str, PortModel]]):
        self.buffers: dict[str, Queue] = {}
        if inputs is not None:
            for name, port in inputs.items():
                buffer: Queue = Queue(maxsize=50)
                self.buffers[name] = buffer

    # TODO: Add a Fan-In strategy

def edge_matches_output_port(node_name, port_name, edge_model: EdgeModel):
    nodes_match = node_name == edge_model.start.node
    ports_match = port_name == edge_model.start.port
    if nodes_match and ports_match:
        return True

class OutGroup:
    """Contains references to the Port buffers in the child nodes"""
    def __init__(self, ports: Optional[dict[str, PortModel]], node_name: str, buffers: dict[EdgeModel, Queue]):
        self.outputs: dict[str, list[Queue]] = {}
        if ports is not None:
            for port_name, port in ports.items():
                found_buffers = [buffer for edge_model, buffer in buffers.items() if edge_matches_output_port(node_name, port_name, edge_model)]
                self.outputs[port_name] = found_buffers


class AsyncNode:
    
    def __init__(self, name: str, model: NodeModel, buffers: dict[EdgeModel, Queue]):
        self.name = name
        self.model = model
        # Parse inputs
        self.ins: InGroup = InGroup(model.inputs)
        self.outs: OutGroup = OutGroup(model.outputs, self.name, buffers)

    async def __call__(self, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
        """This is the function that gets awaited when executing the Flow"""
        task_status.started()
        print(f'Name: {self.name}')
        print(f'input buffers: {self.ins.buffers}')
        print(f'outputs buffers: {self.outs.outputs}')
        print(f'======================')
        return None