import logging
from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, Optional, TypeVar, Union

from anyio import BrokenResourceError, ClosedResourceError, WouldBlock
from anyio.abc import AsyncResource
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mitosis.model.edge_model import SpecificPort

from ..basics.valve import ValveSendStream
from ..model import EdgeModel, PortModel
from ..util import edge_matches_input_port, edge_matches_output_port

MemoryObjectTransmitter = Union[MemoryObjectReceiveStream, MemoryObjectSendStream]

SenderTypes = Union[MemoryObjectSendStream, ValveSendStream]

_LOGGER = logging.getLogger(__name__)


# TODO: Make the receiver constant. Either pass on construction or specifically later
class InputPort(AsyncResource):
    """A port with one, constant receiver."""

    def __init__(
        self,
        node_name: str,
        port_name: str,
        *,
        all_receivers: Optional[dict[SpecificPort, MemoryObjectReceiveStream]] = None,
    ):
        self._data = SpecificPort(node=node_name, port=port_name)

        self._receiver: Optional[MemoryObjectReceiveStream] = None
        if all_receivers is not None:
            self._receiver = self.find_receiver(all_receivers)

    def find_receiver(
        self, all_receivers: dict[SpecificPort, MemoryObjectReceiveStream]
    ) -> Optional[MemoryObjectReceiveStream]:
        """Pick out appropriate receiver."""
        return all_receivers.get(self._data)

    def offer_receivers(
        self, all_receivers: dict[SpecificPort, MemoryObjectReceiveStream]
    ):
        """Assign receiver from dict."""
        self._receiver = self.find_receiver(all_receivers)

    async def get_one(self) -> Any:
        """Get objects from stream."""
        if self._receiver is None:
            raise RuntimeError
        return await self._receiver.receive()

    async def get_all(self) -> list[Any]:
        """Return all objects waiting, but wait for at least one."""
        if self._receiver is None:
            raise RuntimeError
        res: list[Any] = []
        try:
            while True:
                res.append(self._receiver.receive_nowait())
        except WouldBlock:
            pass
        # If none were ready, wait for next one
        if len(res) == 0:
            return [await self._receiver.receive()]
        return res

    async def __aenter__(self):
        """Put all the receiver on the 'stack'."""
        await self._receiver.__aenter__()
        return self

    async def aclose(self):
        """Unwind the 'local stack'."""
        await self._receiver.aclose()


class OutputPort(AsyncResource):
    """A port with a varying number of senders."""

    def __init__(
        self,
        node_name: str,
        port_name: str,
        *,
        all_senders: Optional[dict[SpecificPort, list[MemoryObjectSendStream]]] = None,
    ):
        self._data = SpecificPort(node=node_name, port=port_name)
        self._stack = AsyncExitStack()

        self._senders: list[MemoryObjectSendStream] = []
        if all_senders is not None:
            self._senders = self.find_senders(all_senders)

    def find_senders(
        self, all_senders: dict[SpecificPort, list[MemoryObjectSendStream]]
    ) -> list[MemoryObjectSendStream]:
        """Pick out appropriate receiver."""
        return all_senders[self._data]

    def offer_senders(
        self, all_senders: dict[SpecificPort, list[MemoryObjectSendStream]]
    ):
        """Assign receiver from dict."""
        self._senders = self.find_senders(all_senders)

    def can_send(self) -> bool:
        """Determine if there are any active connections to send to."""
        # If there are no senders, return False
        if len(self._senders) == 0:
            return False
        # If any of the senders are active, return True
        is_active = (
            lambda conn: conn.is_open() if isinstance(conn, ValveSendStream) else True
        )
        if any((is_active(conn) for conn in self._senders)):
            return True
        # If all senders are inactive, return False
        return False

    async def send(self, obj: Any) -> None:
        """Send an object to all streams."""
        if self.can_send():
            for output_stream in self._senders:
                try:
                    await output_stream.send_nowait(obj)
                except BrokenResourceError as exc:
                    # This is probably a persistent cell, trying to send to a Flow which has been shut down.
                    pass  # TODO

    async def __aenter__(self):
        """Put all senders on the stack."""
        for sender in self._senders:
            await self._stack.enter_async_context(sender)
        return self

    async def aclose(self):
        """Unwind the local stack."""
        await self._stack.aclose()


# ===PortGroups=== #


class InGroup(AsyncResource):
    """Contains the node's input stream receivers"""

    def __init__(
        self,
        node_name: str,
        input_ports: dict[str, PortModel],
        all_receivers: Optional[dict[SpecificPort, MemoryObjectReceiveStream]] = None,
    ):
        self._node_name = node_name
        self._stack = AsyncExitStack()
        self._ports: list[InputPort] = [
            InputPort(self._node_name, port_name, all_receivers=all_receivers)
            for port_name, port_model in input_ports.items()
        ]

    # TODO: Pull in as a dict (keys are port names)
    # Then arrange and pass to myfunc through some syntax in the function definition
    async def pull(self):
        """Pull one unit of input to the node function."""
        # Get inputs. For now, just get one element from each input port if available
        # TODO: Add Fan-In strategies
        inputs: list[Any] = []
        for port in self._ports:
            try:
                inputs.append(await port.get_one())
            except ClosedResourceError as exc:
                _LOGGER.error(f"ClosedResourceError!", exc_info=exc)
        return inputs

    async def __aenter__(self):
        """Put all ports on the stack."""
        for port in self._ports:
            await self._stack.enter_async_context(port)
        return self

    async def aclose(self):
        """Unwind the local stack."""
        await self._stack.aclose()


class OutGroup(AsyncResource):
    """Contains references to the Port buffers in the child nodes"""

    def __init__(
        self,
        node_name: str,
        output_ports: dict[str, PortModel],
        all_senders: Optional[dict[SpecificPort, MemoryObjectSendStream]] = None,
    ):
        self._node_name = node_name
        self._stack = AsyncExitStack()
        self._ports: list[OutputPort] = [
            OutputPort(self._node_name, port_name, all_senders=all_senders)
            for port_name, port_model in output_ports.items()
        ]

    def can_send(self) -> bool:
        """Determine if there is anywhere to send output to."""
        return any((port.can_send() for port in self._ports))

    def offer_senders(self, new_attachments) -> bool:
        for port in self._ports:
            port.offer_senders(new_attachments)

    async def push(self, outputs):
        """Push one unit of work to child nodes."""
        # TODO: Proper mapping from myfunc outputs to output ports
        for port, obj in zip(self._ports, [outputs]):
            await port.send(obj)

    async def __aenter__(self):
        """Put all ports on the stack."""
        for port in self._ports:
            await self._stack.enter_async_context(port)
        return self

    async def aclose(self):
        """Unwind the local stack."""
        await self._stack.aclose()
