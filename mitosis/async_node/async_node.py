from contextlib import AsyncExitStack
from typing import Any, Optional

from anyio import (
    TASK_STATUS_IGNORED,
    BrokenResourceError,
    CancelScope,
    Event,
    WouldBlock,
    sleep,
)
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mitosis.model.edge_model import SpecificPort

from ..model import EdgeModel, NodeModel, PortModel
from ..util import edge_matches_input_port, edge_matches_output_port


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
        attachments_receivers: Optional[dict[str, MemoryObjectReceiveStream]] = None,
    ):
        self.name = name
        self.model = model
        # Parse inputs
        self.ins: InGroup = InGroup(self.name, self.model.inputs, receivers)
        self.outs: OutGroup = OutGroup(self.name, self.model.outputs, senders)
        # Do initial check if node should shut down
        self.running: bool = True
        # ReceiveStream for new attached connections
        self._attachments_receiver: Optional[MemoryObjectReceiveStream] = None
        if attachments_receivers is not None:
            self._attachments_receiver = attachments_receivers[self.name]
        # If all senders are empty
        if self.should_stop():
            self.stop()

    def start(self):
        """Start the Node execution."""
        self.running = True

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

    def attach_new_senders(self, new_attachments):
        for output_port_name, port_senders in self.outs.senders.items():
            specific_port = SpecificPort(node=self.name, port=output_port_name)
            if specific_port in new_attachments.keys():
                new_senders = new_attachments[specific_port]
                port_senders.clear()
                port_senders += new_senders

    async def __call__(self, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
        """Run the infinite loop of a node."""

        with CancelScope() as cancel_scope:
            # The CancelScope gets passed back out to caller, so a task can be cancelled from outside
            task_status.started(cancel_scope)

            # Input/Output port setup
            async with AsyncExitStack() as stack:
                for receiver in self.ins.receivers.values():
                    await stack.enter_async_context(receiver)
                for output_port in self.outs.senders.values():
                    for sender in output_port:
                        await stack.enter_async_context(sender)

                while True:
                    # 1: For persistent cells, check if sender pool has changed.
                    # In this case, update senders.
                    # Then check if you should start running, keep running, or keep being dormant.
                    # If not running, wait here to be started again

                    # Need a good primitive for this. An event with one notifier, or a way to show that a cache has been changed
                    # Several processes needs to notice this and take action, but each should only do so once

                    # Use a memory object stream to send the new list of senders. If it should be shutdown, it can receive(), otherwise it can receive_nowait()

                    # Only persistent cells have this
                    if self._attachments_receiver is not None:
                        if not self.running:
                            # If node is not running, it will wait here until new attachments are available
                            new_attachments = await self._attachments_receiver.receive()
                            # When received, attach new senders to output ports.
                            self.attach_new_senders(new_attachments)
                            # Check if there are anywhere to send data now
                            if self.should_stop():
                                continue
                            else:
                                self.start()
                        else:
                            try:
                                new_attachments = (
                                    self._attachments_receiver.receive_nowait()
                                )
                                self.attach_new_senders(new_attachments)
                                # Check if there are anywhere to send data now
                                if self.should_stop():
                                    self.stop()
                                    continue
                                else:
                                    self.start()
                            except WouldBlock:
                                # This is the usual case for a running node
                                pass

                    # Get inputs. For now, just get one element from each input port if available
                    # TODO: Add Fan-In strategies
                    myfunc_inputs: list[Any] = []
                    for receive_stream in self.ins.receivers.values():
                        myfunc_inputs.append(await receive_stream.receive())

                    myfunc_outputs = self.model.get_executable()(*myfunc_inputs)

                    # Push results to child nodes
                    for output_port, e in zip(
                        self.outs.senders.values(), [myfunc_outputs]
                    ):
                        for output_stream in output_port:
                            try:
                                await output_stream.send_nowait(e)
                            except BrokenRessourceError as exc:
                                # This is probably a persistent cell, trying to send to a Flow which has been shut down.
                                

                    # Wait until it is time to run again
                    # TODO: Different strategies for waiting.
                    await sleep(1.0 / self.model.config.frequency)
