import logging
from contextlib import AsyncExitStack
from pathlib import Path

from anyio import create_task_group, run, sleep, sleep_forever

from mitosis import Flow, MitosisApp

_LOGGER = logging.getLogger(__name__)


async def main():
    async with MitosisApp(Path("examples/resources/persistent.json")) as app:
        # Setup stuff here: Start HTTP-client, register flows for later start.
        await sleep(4)

        flow: Flow = app.create_flow(Path("examples/resources/mygraph.json"))

        await app.start_flow(flow)

        print("created")
        await sleep(3)

        print("creating another one")
        flow2: Flow = app.create_flow(Path("examples/resources/mygraph.json"))

        await app.start_flow(flow2)

        await sleep(3)
        print("shutting down the first one")
        await app.stop_flow(flow)  # Cancels its internal TaskGroup
        print("Stopped")


run(main)

## TODO:
## Persistent nodes that can shut down if no one has subscribed to them. Acts as possible inputs to Flows
## Run several Flows simultaneously, allowing for shutting of and spinning up at run time
