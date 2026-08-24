# Example topology harness

`packages/jazz-tools/tests/topology/harness.ts` owns deterministic seeds,
bounded named phases, disconnect/reconnect/restart/failure callbacks, receipts,
and replay commands. App scenarios continue to own schemas, fixtures,
operations, and assertions.

Register a scenario in `dev/example-topology-scenarios.json` with a stable id,
topology labels, working directory, and an argv array. Run the bounded smoke
locally with:

```sh
node dev/gates/run-example-topology-soak.mjs --seed-count 1
```

Copy a failing case's `replay` field from
`target/example-topology-soak/summary.json`. The runner never evaluates shell
strings; registry commands remain explicit argv arrays.

## Open app-branch integration

The product apps are not on `main` yet. Their integration remains deliberately
small and contains no app logic in this harness:

- BandChat (`29d384798`): import `runTopologyScenario` and
  `browserTopologyReporter` in `topology.e2e.test.ts`, express its existing
  disconnect/reconnect/restart calls as fault targets, then register its focused
  Vitest argv.
- BigLabel (`3d6787b3b`): replace its local `browserTopologyPhase` helper with a
  phase in the shared runner and register the focused browser test.
- MusicAgent (`997894311`): use the shared browser reporter for its bounded
  import/server/client phases and register its focused browser topology test.

Those patches should land with their app branches so the public repository does
not copy or prematurely own adopter schemas and workload semantics.
