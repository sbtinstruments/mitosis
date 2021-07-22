from anyio import create_task_group, run, sleep


async def procA():
    while True:
        print("A")
        await sleep(1)


async def procB():
    while True:
        print("B")
        await sleep(1)


async def procC():
    while True:
        print("C")
        await sleep(1)


async def main():
    async with create_task_group() as tg:
        tg.start_soon(procA)
        async with create_task_group() as tg2:
            tg2.start_soon(procB)
            tg2.start_soon(procC)


run(main)
