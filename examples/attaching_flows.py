import logging
from contextlib import AsyncExitStack
from pathlib import Path

from anyio import create_task_group, run, sleep, sleep_forever

from mitosis import Flow, MitosisApp

_LOGGER = logging.getLogger(__name__)


async def main():
    async with create_task_group() as tg, AsyncExitStack() as stack:
        app = MitosisApp(tg, Path("examples/resources/persistent.json"))
        await stack.enter_async_context(app)

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
        await app.stop_flow(flow2)  # Cancels its internal TaskGroup
        print("Stopped")

        await sleep_forever()  # Needed, because there is no implicit join anymore

    print("I am done with the EventLoop")


run(main)

## TODO:
## Persistent nodes that can shut down if no one has subscribed to them. Acts as possible inputs to Flows
## Run several Flows simultaneously, allowing for shutting of and spinning up at run time
