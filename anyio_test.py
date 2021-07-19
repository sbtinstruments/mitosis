import logging
from contextlib import AsyncExitStack
from pathlib import Path

from anyio import (
    TASK_STATUS_IGNORED,
    create_memory_object_stream,
    create_task_group,
    run,
    sleep,
)
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mitosis import Flow, MitosisApp
from mitosis.async_node import AsyncNode
from mitosis.model import EdgeModel, FlowModel

_LOGGER = logging.getLogger(__name__)


async def main():
    async with create_task_group() as tg, AsyncExitStack() as stack:
        app = MitosisApp(tg, Path("mygraph/persistent.json"))
        await stack.enter_async_context(app)

        await sleep(4)

        flow: Flow = app.create_flow(Path("mygraph/mygraph.json"))

        await app.start_flow(flow)

        print("created")
        await sleep(1)

        print("shutting down")
        await app.stop_flow(flow)  # Cancels its internal TaskGroup

        await sleep(4)
        print("starting up again")
        flow2: Flow = app.create_flow(Path("mygraph/mygraph.json"))

        await app.start_flow(flow2)
        print("started ===================")
        # await sleep(5)
        # Implicit join


run(main)

## TODO:
## Persistent nodes that can shut down if no one has subscribed to them. Acts as possible inputs to Flows
## Run several Flows simultaneously, allowing for shutting of and spinning up at run time
