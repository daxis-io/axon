# Current-main worker-pool decision

Status: continue bounded research; do not ship a production worker pool or add WCRPC.

This note separates two different kinds of evidence:

1. a historical controlled timing experiment, which showed enough CPU-bound
   speedup to justify more research; and
2. a fresh compatibility check on current `origin/main`, which proves that two
   independent coordinators can execute disjoint file shards and produce an
   exactly mergeable aggregate without fallback.

Neither result is production-readiness evidence. In particular, neither
measures the process-tree memory cost, startup amortization, cold object-store
behavior, or output-heavy merge pressure of a real pool.

## Historical timing evidence

The last controlled 40-pair experiment ran from the historical worker-pool
research branch at main base `7681f1d`. For P2 versus a single worker it
reported:

- median paired latency ratio: `0.60002`;
- 95% bootstrap interval: `0.57861..0.61081`;
- p90 latency ratio: `0.59847`;
- transferred-byte ratio: `1.000`;
- correctness or hidden-fallback failures: `0`.

That experiment supports continuing the research. It does not establish the
value of the topology now on `origin/main`: it predates the current
coordinator, and its raw benchmark artifacts are deliberately not copied into
the product branch.

## Current-main compatibility evidence

`npm run test:browser:worker-pool-compatibility` exercises the shipped browser
WASM and coordinator against the pinned eight-file `s3-browser-perf-v1`
fixture. It runs the same aggregate once over all eight files and then through
two independent coordinators over deterministic four-file shards.

The first current-main run used source `f20acb7`, WASM SHA-256
`07482846256d5ceabd02f8ac8f278138f1b3b7c554c7210786b73885be2aef1d`,
and Chromium `143.0.7499.4`. It observed:

- single-worker aggregate: `1,048,576` rows, quantity sum `5,239,848`,
  mobile rows `713,690`;
- merged P2 aggregate: exactly the same values;
- shard coverage: all eight files exactly once;
- execution target: `browser_wasm` for all three workers;
- hidden fallback: none;
- Arrow IPC output: nonzero for each worker.

The Playwright attachment is ephemeral test evidence, not a checked-in
benchmark corpus. The test itself is the reproducible proof.

## Decision

| Question                       | Decision     | Reason                                                                                                                                                  |
| ------------------------------ | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Continue worker-pool research? | Yes, bounded | The historical timing signal is large and the current coordinator remains compatible with exact disjoint-file fanout.                                   |
| Ship a production worker pool? | Inconclusive | There is no current-main process-cold timing series, process-tree memory evidence, startup-amortization model, skew study, or output-heavy merge study. |
| Introduce WCRPC?               | No           | The compatibility test needs no new transport, and no measured current-main bottleneck justifies WCRPC's protocol and lifecycle complexity.             |

## Next evidence gate

Do not revisit production work until one compact, reproducible harness records
at least 20 process-cold paired samples for S, P2, and P4 and includes:

- browser plus descendant-worker peak memory, with a documented measurement
  method;
- multiple file counts and a deliberately skewed shard layout;
- both compute-heavy and output-heavy queries;
- startup and steady-state timings reported separately;
- cold public-S3 runs using unique cache identities;
- exact result, file-coverage, byte-transfer, and hidden-fallback checks.

The resulting decision note should retain compact summaries and provenance.
Large raw timing artifacts remain local or in CI artifacts.
