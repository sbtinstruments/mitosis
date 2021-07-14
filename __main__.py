import logging
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

from mitosis import MitosisApp
from mitosis.async_node import AsyncNode
from mitosis.model import EdgeModel, FlowModel

_LOGGER = logging.getLogger(__name__)


async def main():
    async with create_task_group() as tg:
        async with MitosisApp(tg, Path("mygraph/persistent.json")) as app:
            await sleep(4)
            flow_handle = await app.spawn_flow(tg, Path("mygraph/mygraph.json"))
            print("created")
            await sleep(3)
            print("shutting down")
            await flow_handle.shut_down()
            print("here")
            print(flow_handle._cs.shield)


run(main)

## TODO:
## Persistent nodes that can shut down if no one has subscribed to them. Acts as possible inputs to Flows
## Run several Flows simultaneously, allowing for shutting of and spinning up at run time
