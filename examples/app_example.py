import logging
from contextlib import AsyncExitStack
from pathlib import Path

from anyio import create_task_group, run, sleep, sleep_forever

from mitosis import Flow, MitosisApp

_LOGGER = logging.getLogger(__name__)


async def main():

    async with MitosisApp(Path("examples/resources/persistent.json")) as app:
        # Setup stuff here: Start HTTP-client (TODO), register flows for later start.
        path = Path("examples/resources/mygraph.json")
        reg_key = app.register_flow(path, key="measurement1")

        await sleep(4)

        await app.start_flow("measurement1")
        print("created")
        await sleep(10)
        print("shutting down the first one")
        await app.kill_flow("measurement1")
        print("Stopped")
        await sleep(5)
        await app.start_flow("measurement1")
        await sleep_forever()


run(main)
