import logging
from contextlib import AsyncExitStack
from typing import Any, Callable, Optional

from anyio import (
    TASK_STATUS_IGNORED,
    BrokenResourceError,
    CancelScope,
    ClosedResourceError,
    Event,
    WouldBlock,
    current_effective_deadline,
    current_time,
    sleep_until,
)
from anyio.abc import AsyncResource, TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ..model import EdgeModel, NodeModel, PortModel, SpecificPort
from ..util import edge_matches_input_port, edge_matches_output_port
from .port_group import InGroup, OutGroup

_LOGGER = logging.getLogger(__name__)


class AsyncNode(AsyncResource):
    def __init__(
        self,
        name: str,
        model: NodeModel,
        attachments_receivers: Optional[dict[str, MemoryObjectReceiveStream]] = None,
    ):
        self.name = name
        self.model = model
        self._stack = AsyncExitStack()
        self.running: bool = True

        # Executable function
        self._executable: Callable = model.get_executable()

        # Timings
        self._last_run = current_time()
        self._time_between_runs = 1.0 / model.config.frequency  # [s]

        # Default Input/Output-groups TODO: Break these out into their own component
        self.ins = InGroup(self.name, model.inputs)
        self.outs = OutGroup(self.name, model.outputs)

        # ReceiveStream for new attached connections
        self._attachments_receiver: Optional[MemoryObjectReceiveStream] = None
        if attachments_receivers is not None:
            self._attachments_receiver = attachments_receivers[self.name]
        # If all senders are empty
        if self.should_stop():
            self.stop()

    def offer_buffers(
        self,
        senders: dict[SpecificPort, MemoryObjectSendStream],
        receivers: dict[SpecificPort, MemoryObjectReceiveStream],
    ) -> None:
        """Pass buffers in and create new input/output-groups."""
        self.ins = InGroup(self.name, self.model.inputs, receivers)
        self.outs = OutGroup(self.name, self.model.outputs, senders)

    def start(self):
        """Start the Node execution."""
        self.running = True

    def should_stop(self):
        """Check if a node should stop execution."""
        if (
            self.model.config.shut_down_when_ignored is True
            and self.outs.can_send() is False
        ):
            return True
        return False

    def stop(self):
        """Stop the Node execution."""
        self.running = False

    async def attach_new_senders(self, new_attachments):
        self.outs.offer_senders(new_attachments)

    async def __aenter__(self):
        """Put all senders and receivers on the stack."""
        await self._stack.enter_async_context(self.ins)
        await self._stack.enter_async_context(self.outs)
        return self

    async def aclose(self):
        """Unwind the local stack."""
        await self._stack.aclose()

    async def __call__(self, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
        """Run the infinite loop of a node."""
        with CancelScope() as scope:
            task_status.started(scope)
            while True:
                # Starting and stopping is idempotent
                if self.should_stop():
                    self.stop()
                else:
                    self.start()

                if self._attachments_receiver is not None:
                    if not self.running:
                        # If node is not running, it will wait here until new attachments are available
                        new_attachments = await self._attachments_receiver.receive()
                        # When received, attach new senders to output ports.
                        await self.attach_new_senders(new_attachments)
                        # Check if there are anywhere to send data now
                        continue

                    # If node is running, just check (nowait) if there are any new attachments
                    try:
                        new_attachments = self._attachments_receiver.receive_nowait()
                    except WouldBlock:
                        # This is the usual case for a running node
                        pass
                    else:
                        await self.attach_new_senders(new_attachments)
                        continue

                myfunc_inputs = await self.ins.pull()
                # Run executable code
                myfunc_outputs = self._executable(*myfunc_inputs)
                # Push results to child nodes.
                await self.outs.push(myfunc_outputs)

                # Wait until it is time to run again
                # TODO: Different strategies for waiting.
                await sleep_until(self._last_run + self._time_between_runs)
                self._last_run = current_time()
