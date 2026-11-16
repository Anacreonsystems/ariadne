# Ariadne native streaming

The current source research for the new engine is tracked in the
[Apache Flink architecture study](research/flink-source-study/README.md).
It documents a pinned source revision and confirmed reading ranges; the study
is in progress and does not replace the historical acceptance records below.

Start with the [finite streaming SDK guide](sdk_guide.md) for ordinary Prefect
Tasks, an explicit A1→B1/C2 graph, reference payloads and deployment/worker setup.
The walkthrough distinguishes current real-process evidence from the remaining
acceptance work; it does not mark any ADR accepted.

Use the [epoch 2 plan](epochs/epoch-02-native-streaming-completion.md),
[ADR index](adr/README.md), [task registry](todo.md) and
[sprint board](sprint_board.md) to follow the architecture/evidence graph.
Historical experiments remain in [prototypes](prototypes/README.md); their
interfaces are separate from the native SDK documented in the guide.
