from flow_model.model import FlowModel
from typing import Optional, Any
from anyio import TASK_STATUS_IGNORED, sleep
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream


from contextlib import AsyncExitStack

from flow_model import NodeModel, PortModel, EdgeModel


def edge_matches_input_port(node_name, port_name, edge_model: EdgeModel):
    nodes_match = node_name == edge_model.end.node
    ports_match = port_name == edge_model.end.port
    if nodes_match and ports_match:
        return True


def edge_matches_output_port(node_name, port_name, edge_model: EdgeModel):
    nodes_match = node_name == edge_model.start.node
    ports_match = port_name == edge_model.start.port
    if nodes_match and ports_match:
        return True


class InGroup:
    """Contains the node's input stream receivers"""

    def __init__(
        self,
        node_name: str,
        input_ports: Optional[dict[str, PortModel]],
        all_receivers: dict[EdgeModel, MemoryObjectReceiveStream],
    ):
        self.receivers: dict[str, MemoryObjectReceiveStream] = {}
        if input_ports is not None:
            # For each input port
            for port_name, port in input_ports.items():
                # Find appropriate edge
                for edge_model in all_receivers.keys():
                    if edge_matches_input_port(node_name, port_name, edge_model):
                        print("Found one!")
                        self.receivers[port_name] = all_receivers[edge_model]

    # TODO: Add Fan-In strategies
    async def get(self):
        res: list[list[Any]] = []
        for receive_stream in self.receivers.values():
            elems = []
            async for item in receive_stream:
                elems.append(item)
            # TODO: For testing, we just sum inputs from every port
            res.append(sum(elems))
        return res


class OutGroup:
    """Contains references to the Port buffers in the child nodes"""

    def __init__(
        self,
        node_name: str,
        output_ports: Optional[dict[str, PortModel]],
        all_senders: dict[EdgeModel, MemoryObjectReceiveStream],
    ):
        self.senders: dict[str, list[MemoryObjectSendStream]] = {}
        if output_ports is not None:
            for port_name, port in output_ports.items():
                found_send_streams = [
                    send_stream
                    for edge_model, send_stream in all_senders.items()
                    if edge_matches_output_port(node_name, port_name, edge_model)
                ]
                self.senders[port_name] = found_send_streams


class AsyncNode:
    def __init__(
        self,
        name: str,
        model: NodeModel,
        senders: dict[EdgeModel, MemoryObjectSendStream],
        receivers: dict[EdgeModel, MemoryObjectReceiveStream],
    ):
        self.name = name
        self.model = model
        # Parse inputs
        self.ins: InGroup = InGroup(self.name, self.model.inputs, receivers)
        self.outs: OutGroup = OutGroup(self.name, self.model.outputs, senders)

    async def __call__(self, *, task_status: TaskStatus = TASK_STATUS_IGNORED):
        """This is the function that gets awaited when executing the Flow"""
        task_status.started()
        print(f"Name: {self.name}")
        print(f"input buffers: {self.ins.receivers}")
        print(f"outputs buffers: {self.outs.senders}")
        print(f"&&&&&&&&&&&&&&&&&&&&&")
        # return None

        while True:

            # Input/Output port setup
            async with AsyncExitStack() as stack:
                print(f"[{self.name}] before input port setup")
                for receiver in self.ins.receivers.values():
                    await stack.enter_async_context(receiver)
                print(f"[{self.name}] after input port setup")
                for output_port in self.outs.senders.values():
                    for sender in output_port:
                        await stack.enter_async_context(sender)
                print(f"[{self.name}] after output port setup")

                while True:

                    # Get inputs. For now, just get one element from each input port if available
                    myfunc_inputs: list[Any] = []
                    for receive_stream in self.ins.receivers.values():
                        myfunc_inputs.append(await receive_stream.receive())

                    myfunc_outputs = self.model.executable.code['main'](*myfunc_inputs)

                    # Push results to child nodes
                    for output_port, e in zip(self.outs.senders.values(), [myfunc_outputs]):
                        for output_stream in output_port:
                            await output_stream.send_nowait(e)
                    # Wait until it is time to run again
                    if self.name == "D":
                        print(self.ins.receivers['FloatIn'].statistics())
                        await sleep(1)
                    else:
                        await sleep(0.5)

        # print(self.ins.receivers.values())
        # async with self.ins.receivers.values():
        #     while True:
        #         # raw_string_queue = self.ins.buffers["RawString"]
        #         # data = raw_string_queue.get(block=True)
        #         # I want to depend on another port: data = await raw_string_queue.get()
        #         # data = raw_string_queue.get_nowait()
                
        #         # Get inputs from input buffers
        #         inputs = await self.ins.get()
        #         # Perform node-specific calculations
        #         if self.name == "A":
        #             execute_func = lambda: 1
        #         elif self.name == "B":
        #             execute_func = lambda x: 2 * x
        #         elif self.name == "C":
        #             execute_func = lambda y: 3 * y
        #         elif self.name == "D":
        #             execute_func = lambda x, y: print(x + y)
        #         outputs = execute_func(*inputs)
        #         # Push results to child nodes
        #         for output_port, e in zip(self.outs.senders.values(), [outputs]):
        #             for output in output_port:
        #                 await output.send_nowait(e)
        #         # Wait until it is time to run again
        #         await sleep(2)
