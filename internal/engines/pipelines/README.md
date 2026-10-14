# pipelines

The package at `internal/engines/pipelines` runs FlowG's pipelines. A pipeline
is authored in the UI as a flow graph; this engine compiles that graph into an
executable node graph and drives log records through it.

The engine is built on a single [actor](https://github.com/vladopajic/go-actor)
that owns the set of running pipeline builds. Pipelines are eager: every
persisted pipeline is compiled and started at boot, and the
[confignotify](../confignotify) events emitted by the config storage rebuild or
terminate builds as the configuration changes, wherever the change was made.

## Responsibilities

- **Compilation** — turns a stored `FlowGraphV2` into a graph of `Node`s,
  resolving the transformers each node references and building a
  [forwarders](../forwarders) runtime for each forward node.
- **Execution** — feeds a record into a chosen entrypoint and propagates it
  through the graph, concurrently fanning out to each node's successors.
- **Lifecycle** — starts every persisted pipeline at boot tagged with its
  configuration version (a broken pipeline is exposed as an error and keeps the
  server up), swaps in new builds on versioned `PipelineChanged` events,
  retires builds on `PipelineDeleted`, and reconciles everything on
  `DependenciesChanged`.
- **Convergence status** — exposes `Runner.Status`: for every pipeline the
  running/pending/error phase, the version currently serving and the desired
  version, surfaced over HTTP at `GET /api/v1/status/pipelines`.
- **Tracing** — supports dry runs that record what each node received, emitted
  and errored, without performing side effects.
- **Wiring** — `NewRunner` returns an `fx` module providing a `Runner` bound to
  the application lifecycle.

## Versioned convergence

Each pipeline has an actor-owned state tracking the `DesiredVersion` (highest
version observed for it), the `ActiveVersion` compiled into the build currently
serving records, in-flight candidate builds and the last failure.

Events carry the configuration version committed by storage:

- a `PipelineChanged` at a version already desired is a duplicate (repeated
  notification or watch reconnection) and is ignored;
- candidate builds run on detached goroutines — storage reads, VRL compilation
  and forwarder connects never block the actor loop — and report back tagged
  with their version;
- a result older than the desired version is discarded and its candidate
  closed, so an out-of-order build can never overwrite a newer one;
- a failed candidate at the desired version keeps the previous build serving
  records and marks the instance `error` (with the failure reason); when no
  previous build exists the instance is not serving; a newer build marks it
  `pending` and a success switches it back to `running`;
- on `DependenciesChanged` every listed pipeline is rebuilt independently —
  one pipeline failing never blocks the others — while builds missing from
  storage are retired; the dependency cursor makes repeated reconciles
  idempotent and failed snapshots retry on a timer;
- `PipelineDeleted` removes the state and closes the build; a candidate that
  finishes just after deletion is discarded and never resurrects it.

At boot the runner reads the global and per-item versions, builds every
persisted pipeline concurrently and seeds the actor once, so the service
starts ready; a later crash before a notification is enqueued is healed by
this version comparison with the (empty) in-memory state. Restore bumps the
storage version and forces a full reconcile, rebuilding survivors and closing
pipelines that disappeared.

## Build lifetime

Every use of a running build (a record being processed, a metrics scrape, a
nested pipeline call) holds an in-flight reservation on it. Retiring a build —
on update, deletion or shutdown — first marks it closed so new reservations
fail, then waits for the in-flight ones to drain before closing node resources
(forwarder connections, metric collectors). On update, in-flight records finish
on the old build while new records already flow through the new one; if the new
build fails to compile, the old one keeps running. The drain wait is bounded by
the caller's context, but the teardown itself always completes in the
background.

Every stage — notification, candidate build, build failure, instance switch,
previous-build close, reconcile and deletion — logs the configuration version
(and the pipeline name) so one change can be traced end to end.

## Layout

- **main.go** — the `Runner` interface (`Process`, `ScrapMetrics`, `Status`),
  its actor-backed implementation, the synchronous boot build path, the
  terminate lifecycle and the `fx` wiring (subscription to config events,
  graceful stop).
- **worker.go** — the actor body; owns the convergence states and running
  builds, the detached build dispatch, version guards, acquire/retire lifetime
  logic and the event-driven reconcile with retry.
- **status.go** — the `PipelineStatus`/`PipelinePhase` model and the
  actor-exclusive per-pipeline state.
- **messages.go** — the actor messages (process, scrap metrics, terminate,
  build result, status query, reconcile snapshot and retry, boot seed) and the
  entrypoint constants.
- **types_pipeline.go** — `Pipeline` and the `BuildFlow`/`BuildFromStorage`
  compilers.
- **types_nodes.go** — the `Node` interface and the node implementations
  (source, transform, switch, pipeline, forward, router).
- **node_tracer.go** — the dry-run tracer carried through the context.
- **context.go** — context plumbing used to reach the worker from inside nodes.
- **errors.go** — the typed errors raised while compiling a flow graph.
- **mocks/** — a testify mock of `Runner` for tests.

## Node types

A compiled pipeline is a directed graph of nodes:

- **source** — an entrypoint; forwards records to its successors. Each source's
  declared type (e.g. `direct`, `syslog`) names an entrypoint.
- **transform** — runs a VRL transformer, which may emit zero, one or many
  records per input.
- **switch** — forwards a record only when it matches an
  [expr](https://expr-lang.org/) condition.
- **pipeline** — delegates processing to another named pipeline.
- **forward** — sends the record to an external destination through a forwarder.
- **router** — a terminal node; persists the record to a stream and notifies
  live subscribers.

## Dry runs

When a `NodeTracer` is attached to the context (via `WithTracer`), processing
switches to dry-run mode: side-effecting nodes (forward, router, nested
pipeline) skip their effects and every node appends a trace entry
(`models.PipelineNodeTrace`), letting the UI show exactly how a record would
travel through the pipeline.
