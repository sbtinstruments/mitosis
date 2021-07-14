from contextlib import AsyncExitStack
from typing import Any, Optional

from anyio import TASK_STATUS_IGNORED, Event, sleep
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..model import EdgeModel, NodeModel, PortModel


def edge_matches_input_port(node_name, port_name, edge_model: EdgeModel):
    nodes_match = node_name == edge_model.end.node
    ports_match = port_name == edge_model.end.port
    if nodes_match and ports_match:
        return True


def edge_matches_output_port(node_name, port_name, edge_model: EdgeModel):
    nodes_match = node_name == edge_model.start.node
    ports_match = port_name == edge_model.start.port
    if nodes_match and ports_match:
        return True


class InGroup:
    """Contains the node's input stream receivers"""

    def __init__(
        self,
        node_name: str,
        input_ports: Optional[dict[str, PortModel]],
        all_receivers: dict[EdgeModel, MemoryObjectReceiveStream],
    ):
        self.receivers: dict[str, MemoryObjectReceiveStream] = {}
        if input_ports is not None:
            # For each input port
            for port_name in input_ports.keys():
                # Find appropriate edge
                for edge_model in all_receivers.keys():
                    if edge_matches_input_port(node_name, port_name, edge_model):
                        print("Found one!")
                        self.receivers[port_name] = all_receivers[edge_model]


class OutGroup:
    """Contains references to the Port buffers in the child nodes"""

    def __init__(
        self,
        node_name: str,
        output_ports: Optional[dict[str, PortModel]],
        all_senders: dict[EdgeModel, MemoryObjectSendStream],
    ):
        self.senders: dict[str, list[MemoryObjectSendStream]] = {}
        if output_ports is not None:
            for port_name in output_ports.keys():
                found_send_streams = [
                    send_stream
                    for edge_model, send_stream in all_senders.items()
                    if edge_matches_output_port(node_name, port_name, edge_model)
                ]
                self.senders[port_name] = found_send_streams


class AsyncNode:
    def __init__(
        self,
        name: str,
        model: NodeModel,
        senders: dict[EdgeModel, MemoryObjectSendStream],
        receivers: dict[EdgeModel, MemoryObjectReceiveStream],
    ):
        self.name = name
        self.model = model
        # Parse inputs
        self.ins: InGroup = InGroup(self.name, self.model.inputs, receivers)
        self.outs: OutGroup = OutGroup(self.name, self.model.outputs, senders)
        # Do initial check if node should shut down
        self.running: bool = True
        self.resume: Optional[Event] = None
        # If all senders are empty
        if self.should_stop():
            self.stop()

    def start(self):
        """Start the Node execution."""
        self.running = True
        self.resume.set()

    def should_stop(self):
        """Check if a node should stop execution."""
        if self.model.config.shut_down_when_ignored is True and all(
            len(v) == 0 for v in self.outs.senders.values()
        ):
            return True
        return False

    def stop(self):
        """Stop the Node execution."""
        self.running = False
        self.resume = Event()

    async def __call__(self, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
        """Run the infinite loop of a node."""
        task_status.started()

        # Input/Output port setup
        async with AsyncExitStack() as stack:
            for receiver in self.ins.receivers.values():
                await stack.enter_async_context(receiver)
            for output_port in self.outs.senders.values():
                for sender in output_port:
                    await stack.enter_async_context(sender)

            while True:
                # If not running, wait here to be started again
                if not self.running and self.resume is not None:
                    await self.resume.wait()

                # Get inputs. For now, just get one element from each input port if available
                # TODO: Add Fan-In strategies
                myfunc_inputs: list[Any] = []
                for receive_stream in self.ins.receivers.values():
                    myfunc_inputs.append(await receive_stream.receive())

                myfunc_outputs = self.model.get_executable()(*myfunc_inputs)

                # Push results to child nodes
                for output_port, e in zip(self.outs.senders.values(), [myfunc_outputs]):
                    for output_stream in output_port:
                        await output_stream.send_nowait(e)

                # Wait until it is time to run again
                # TODO: Different strategies for waiting.
                await sleep(1.0 / self.model.config.frequency)
