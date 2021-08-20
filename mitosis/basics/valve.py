from dataclasses import dataclass
from typing import Any

from anyio import create_memory_object_stream
from anyio.abc import ObjectReceiveStream, ObjectSendStream


class ValveClosedError(Exception):
    pass


class ValveState:
    """A closeable MemoryObjectStream"""

    def __init__(self, **kwargs):
        self._is_open: bool = True
        self.raw_sender, self.raw_receiver = create_memory_object_stream(**kwargs)

    # ===Send/Receive===#

    async def send(self, obj: Any):
        if self._is_open is True:
            await self.raw_sender.send(obj)
        else:
            raise ValveClosedError

    def send_nowait(self, obj: Any):
        if self._is_open is True:
            self.raw_sender.send_nowait(obj)
        else:
            raise ValveClosedError

    async def receive(self):
        await self.raw_receiver.receive()

    def receive_nowait(self):
        self.raw_receiver.receive_nowait()

    # ===Aenter/Aexit===#

    async def aenter_send(self):
        self.raw_sender.__aenter__()

    async def aexit_send(self, exc_type, exc, tb):
        await self.raw_sender.__aexit__(self, exc_type, exc, tb)

    async def aenter_receive(self):
        self.raw_receiver.__aenter__()

    async def aexit_receive(self, exc_type, exc, tb):
        await self.raw_receiver.__aexit__(self, exc_type, exc, tb)

    # ===Control===#
    @property
    def is_open(self):
        return self._is_open

    @is_open.setter
    def is_open(self, new_state: bool):
        if self._is_open == new_state:
            return
        elif new_state is True:
            self._open()
        elif new_state is False:
            self._close()
        else:
            return

    def _open(self):
        self._is_open = True

    def _close(self):
        self._is_open = False


@dataclass
class ValveSendStream:
    valve_state: ValveState

    async def send(self, obj: Any) -> None:
        await self.valve_state.send(obj)

    def send_nowait(self, obj: Any) -> None:
        self.valve_state.send_nowait(obj)

    def is_open(self) -> bool:
        return self.valve_state.is_open()

    async def __aenter__(self):
        await self.valve_state.aenter_send()

    async def __aexit__(self, exc_type, exc, tb):
        await self.valve_state.aexit_send(self, exc_type, exc, tb)


@dataclass
class ValveReceiveStream:
    valve_state: ValveState

    async def receive(self):
        await self.valve_state.receive()

    def receive_nowait(self):
        self.valve_state.receive_nowait()

    async def __aenter__(self):
        await self.valve_state.aenter_receive()

    async def __aexit__(self, exc_type, exc, tb):
        await self.valve_state.aexit_receive(self, exc_type, exc, tb)


@dataclass
class ValveController:
    valve_state: ValveState

    def open(self):
        self.valve_state.open()

    def close(self):
        self.valve_state.close()

    def state(self):
        self.valve_state.state()


def create_valve(**kwargs):
    valve: ValveState = ValveState(**kwargs)
    return (ValveSendStream(valve), ValveReceiveStream(valve), ValveController(valve))
