from enum import Enum, auto
from datetime import datetime, timedelta
from anyio import sleep
from queue import Queue

from statistics import mean

class AsyncNodeState(Enum):
    OFF = auto()
    READY = auto()
    RUNNING = auto()
    SHUTTING_DOWN = auto()
    ERROR = auto()

class AveragerNode:
    """An async node in the graph. This represents a time domain. It runs as a separate async process"""

    def __init__(self, output_freq: float):
        self._output_freq = output_freq
        
        self._last_run = datetime.utcfromtimestamp(0)
        self._wait_time = timedelta(seconds=1/output_freq)

        # Input buffer
        self._buffer = Queue(30)
        
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        pass


    # Buffer access
    def pop_all_buffer(self):
        buffer = []
        while self._buffer.empty() is not True:
            buffer.append(self._buffer.get())
        return buffer
    
    def put_buffer(self, e):
        self._buffer.put(e)

    async def run_method(self):
        while True:
            self._last_run = datetime.now()
            buffer_contents = self.pop_all_buffer()
            print(f'Averager -- Buffer: {buffer_contents}')
            if len(buffer_contents)>0:
                avg = mean(buffer_contents)
                print(f'Mean: {avg}')
            await sleep((self._last_run + self._wait_time - datetime.now()).total_seconds())

    


class ProducerNode:
    """An async node in the graph. This represents a time domain. It runs as a separate async process"""

    def __init__(self, output_freq: float):
        self._output_freq = output_freq
        
        self._last_run = datetime.utcfromtimestamp(0)
        self._wait_time = timedelta(seconds=1/output_freq)

        # Child
        self._child = None

    def add_child(self, child: AveragerNode):
        self._child = child
        
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def run_method(self):
        while True:
            self._last_run = datetime.now()
            if self._child is not None:
                self._child.put_buffer(3)
            await sleep((self._last_run + self._wait_time - datetime.now()).total_seconds())

    