import logging
from collections import defaultdict
from contextlib import AsyncExitStack
from pathlib import Path
from typing import AsyncContextManager, Hashable, Optional

from anyio import create_task_group
from anyio.streams.memory import MemoryObjectSendStream

from mitosis.basics.exceptions import KeyNotPresentException, KeyNotUniqueException
from mitosis.flow.flow import LinkedFlowState

from ..flow import Flow, FlowHandle, LinkedFlow
from ..model import FlowModel, SpecificPort
from ..util import edge_matches_output_port

_LOGGER = logging.getLogger(__name__)


def add_to_attachments_stash(
    attachments: dict[SpecificPort, list[MemoryObjectSendStream]],
    linked_flow: LinkedFlow,
) -> None:
    """Add send_streams from the flow to the attachments."""
    flow = linked_flow.flow
    if flow.model.externals is not None:
        for external_port in flow.model.externals.connections:
            # Find all connections to that external port
            found_send_streams = linked_flow._senders[external_port]
            if attachments.get(external_port) is None:
                attachments[external_port] = found_send_streams
            else:
                attachments[external_port] += found_send_streams


def remove_from_attachments_stash(
    attachments: dict[SpecificPort, list[MemoryObjectSendStream]],
    linked_flow: LinkedFlow,
) -> None:
    """Remove send_streams belonging to the flow from the attachments."""
    flow = linked_flow.flow
    if flow.model.externals is not None:
        for external_port in flow.model.externals.connections:
            # Find all connections to that external port
            found_send_streams = linked_flow._senders[external_port]
            current_attachments = attachments[external_port]
            attachments[external_port] = [
                sender
                for sender in current_attachments
                if sender not in found_send_streams
            ]


class FlowManager(AsyncContextManager):
    """Manager for a set of runtime flows."""

    def __init__(self):
        self._stack = AsyncExitStack()
        self._tg = create_task_group()

        self.saved_flows: dict[Hashable, Flow] = {}
        self.running_flows: dict[Hashable, FlowHandle] = {}

        # A stash of senders, representing active connections from persistent cells to Flows.
        # Whenever a flow starts or stops, the persistent cells must update themselves from this.
        # This is NOT up to this manager but must be handled by whoever creates an instance of this class
        self._attachments: dict[
            SpecificPort, list[MemoryObjectSendStream]
        ] = defaultdict(list)

    async def __aenter__(self):
        """Enter async context. Creates own taskgroup."""
        await self._stack.enter_async_context(self._tg)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit async context. Will cancel all child tasks."""
        await self._tg.cancel_scope.cancel()
        await self._stack.__aexit__(exc_type, exc, tb)

    def _compile_flow(self, path: Path) -> Flow:
        """Compile a Flow object, creating all the separate nodes. This performs validation, but does not create any comms streams."""
        flow_model = FlowModel.parse_file(path)
        return Flow(flow_model)

    def _link_flow(self, flow: Flow) -> LinkedFlow:
        """Links a flow object, creating the comms streams. These are not reentrant, so this must be done again every time a flow is started/loaded."""
        return LinkedFlow(flow)

    async def _load_flow(self, linked_flow: LinkedFlow) -> FlowHandle:
        """Load/Start a Flow, giving back the needed ID/Flowhandle for identification."""
        return await FlowHandle.from_linked_flow(linked_flow, self._tg)

    def register_flow(self, path: Path, key: Optional[Hashable] = None) -> Hashable:
        """Compile and register a flow."""
        # Check if key is already in use
        flow_key = path if key is None else key
        if flow_key in self.saved_flows:
            raise KeyNotUniqueException(flow_key)
        # Create flow
        flow = self._compile_flow(path)
        # Register flow
        self.saved_flows[flow_key] = flow
        # Return used key
        return flow_key

    async def _spawn_flow(self, flow: Flow) -> FlowHandle:
        """Link and load a flow. Assumes flow is already saved."""
        # Link
        linked_flow = self._link_flow(flow)
        # Start/Load
        flow_handle = await self._load_flow(linked_flow)
        # Update stash of external connections.
        add_to_attachments_stash(self._attachments, flow_handle.linked_flow)
        return flow_handle

    async def _kill_flow(self, key: Hashable):
        """Completely remove a flow from execution."""
        # Get flow handle
        try:
            flow_handle = self.running_flows[key]
        except KeyError as exc:
            _LOGGER.warning(
                f"The flow at {key} cannot be stopped, since it is not running."
            )
            return
        # Detach Flow from external connections.
        remove_from_attachments_stash(self._attachments, flow_handle.linked_flow)
        # Exit all streams
        await flow_handle.linked_flow.aclose()
        # Stop processes immediately
        await flow_handle.scopes.cancel_all()
        # Deregister flow
        del self.running_flows[key]

    async def start_flow(self, key: Hashable) -> None:
        """Spawn flow if not running, activate if inactive."""
        # Check if flow is saved
        try:
            flow = self.saved_flows[key]
        except KeyError as exc:
            raise KeyNotPresentException from exc
        # Check if linked flow is already running
        if key in self.running_flows:
            flow_handle = self.running_flows[key]
            flow_state = flow_handle.linked_flow.state
            if flow_state == LinkedFlowState.INACTIVE:
                # TODO: activate flow
                pass
            else:
                return
        else:
            # Spawn flow
            flow_handle = await self._spawn_flow(flow)
            # Register flow handle
            self.running_flows[key] = flow_handle

    async def start_flow_from_filepath(self, path: Path, key=None):
        """Start a flow from a filepath."""
        used_key = self.register_flow(path, key)
        await self.start_flow(used_key)

    async def stop_flow(self, key: Hashable) -> None:
        """Deactivate flow."""
        raise NotImplementedError

    async def kill_flow(self, key: Hashable) -> None:
        """Kill flow forcefully."""
        await self._kill_flow(key)

    def get_attachments(self):
        """Return current set of attachments for the Persistent Cells."""
        return self._attachments
