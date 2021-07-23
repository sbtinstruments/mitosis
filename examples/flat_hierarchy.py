import logging
from contextlib import AsyncExitStack
from pathlib import Path

from anyio import create_task_group, run, sleep, sleep_forever

from mitosis import Flow, MitosisApp

_LOGGER = logging.getLogger(__name__)


async def main():
    async with MitosisApp(Path("examples/resources/persistent.json")) as app:
        # Setup stuff here: Start HTTP-client, register flows for later start.
        path = Path("examples/resources/mygraph.json")
        await sleep(4)

        await app.start_flow(path, key="measurement1")
        print("created")
        await sleep(3)
        print("creating another one")
        await app.start_flow(path, key="measurement2")
        await sleep(3)
        print("shutting down the first one")
        await app.stop_flow("measurement1")
        print("Stopped")
        await sleep(3)


run(main)
