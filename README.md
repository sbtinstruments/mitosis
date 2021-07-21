# Mitosis

## An Async Orchestrator for Flow-Based Programming in Python

Mitosis is built on AnyIO and lets you:
- Orchestrate dataflows in a graph-based manner (actually [flow-based](https://en.wikipedia.org/wiki/Flow-based_programming)) as data passed between computation nodes
- Exert runtime control over which computations are performed
- Automatically switch off data-providing source-nodes when no one is requesting data

All the nodes can run in different time-domains, so a fast node sending data to a slower node will simply buffer up the data until it is needed. Conversely, a fast node will simply wait for new input data.

### When should you use Mitosis?
Mitosis is designed for devices with several sensors, IoT devices, etc. It is designed for (soft) real-time applications.

### Running the examples
Mitosis uses Poetry to manage dependencies. In the Poetry shell, run the example scripts with the `unittest` module to avoid installing the package first, e.g.
`python3.9 -m unittest examples/attaching_flows.py`

### Other projects
Many other projects have implemented Flow-Based principles, also in Python. The project that comes closest to the goals of Mitosis is [Pyperator](https://github.com/baffelli/pyperator) which now, sadly, seems to not be developed anymore.

### Disclaimer
Mitosis is a WORK-IN-PROGRESS and not meant for production use yet. Suggestions and questions are very much encouraged!