from anyio import create_task_group, move_on_after, sleep, run, TASK_STATUS_IGNORED, CancelScope
from anyio.abc import TaskStatus


class A:
    async def __call__(self, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
      with CancelScope() as scope:
        task_status.started(scope)
        print("Starting sleep")
        await sleep(5)
        print("This should never be printed")


async def main():
    async with create_task_group() as tg:
      a = A()
      a_scope = await tg.start(a)
      await sleep(2)
      a_scope.cancel()
      # The cancel_called property will be True if timeout was reached
      print("Exited cancel scope, cancelled =", a_scope.cancel_called)


run(main)
