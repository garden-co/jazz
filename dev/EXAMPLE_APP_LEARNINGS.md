# Example App Learnings

This is the untriaged inbox for surprises encountered while building the
canonical apps in the [Examples & Benchmarks Program](./EXAMPLES_AND_BENCHMARKS_PROGRAM.md).
These observations are valuable precisely because the app lanes are our
closest repeatable proxy for a fresh adopter.

Keep an item here until one of these exit conditions is met:

1. the underlying problem is fixed;
2. a durable test captures the intended behavior; or
3. the unresolved product choice becomes an explicit open question in a spec.

When an item exits, replace it with a short link to its fix, test, or spec
question before removing it in a later cleanup. Do not silently discard an
observation because an app found a workaround. We will periodically triage
this inbox; entries do not need an owner or proposed solution when first
recorded.

## Untriaged

### Omitted mutation policies appear to be accepted by deployed authorities

- **Observed in:** BandChat and BigLabel deployed-local-server probes.
- **Surprise:** schema validation warns that omitted insert, update, or delete
  policies default to deny, but authority-edge writes using omitted actions
  resolved successfully.
- **Impact:** BandChat allowed room self-admission and forged authorship;
  BigLabel allowed tenant self-admission, role escalation, and foreign
  mutations until every action received an explicit policy.
- **Do not close with:** example-local explicit policies alone. The validator
  and runtime security contract must agree and gain a minimized core test.

### No reusable deployed topology and adversarial-policy harness

- **Observed in:** BandChat and BigLabel authorization tests.
- **Surprise:** each app must assemble local server deployment, schemas,
  principals, edge connections, rejection assertions, teardown, and fault
  schedules itself.
- **Impact:** app tests are expensive to author, easy to make subtly different,
  and initially drifted toward fixture-only checks that never exercised an
  authority.
- **Desired evidence:** a shared public harness that still lets each app own
  its scenario semantics.

### Fixture-only isolation tests can become tautological

- **Observed in:** the first BigLabel scenario receipt.
- **Surprise:** filtering a deterministic fixture to the current tenant and
  then checking that the filtered result has no foreign rows looks like a
  security test but cannot detect a policy failure.
- **Impact:** strong-looking coverage can pass without deploying policies or
  contacting an enforcing authority.
- **Desired evidence:** deployed foreign read/write attempts plus a planted
  policy regression that makes the test fail.

### BandChat fresh-store server delivery does not complete

- **Observed in:** a stronger BandChat bootstrap/replay scenario attempted
  after the initial review.
- **Surprise:** deployed authorization and local persistence pass, but a fresh
  reader did not receive the bootstrapped room/message state from the server
  within the focused test deadline.
- **Impact:** BandChat cannot yet claim a real server-sync or offline-to-online
  delivery topology even though its local reopen path works.
- **Do not close with:** a larger timeout or a local-only substitute. Determine
  whether the app misuses bootstrap APIs or the integration core has a replay
  defect, then preserve the result as a topology test.

### New workspace examples need manual lockfile and package linking work

- **Observed in:** BandChat and BigLabel setup.
- **Surprise:** adding a workspace package does not make its scripts runnable
  through the normal frozen install until the root lockfile importer is
  regenerated; cached dependencies alone do not create package-local links.
- **Impact:** a fresh contributor encounters missing modules or
  `ERR_PNPM_OUTDATED_LOCKFILE` before app code can run.
- **Possible destination:** example scaffolding automation and a focused CI
  contract for workspace registration.

### Pure fixture tests can accidentally start Jazz development infrastructure

- **Observed in:** initial BigLabel Vitest configuration.
- **Surprise:** using the Jazz Vite plugin for pure deterministic fixture tests
  started a local server, encountered stale app/schema state, and held a test
  process open after assertions completed.
- **Impact:** slower and stateful tests, confusing migration warnings, and a
  roughly ten-second close delay unrelated to the fixture behavior.
- **Current workaround:** BigLabel separates pure tests from Jazz-powered
  integration tests. Keep the learning until the default test setup or
  documentation makes this boundary obvious.

### Browser tests cannot import Node-only global setup for shared constants

- **Observed in:** BandChat browser tests.
- **Surprise:** importing a global-setup module merely to share configuration
  caused the browser bundler to traverse the Node-only `JazzServer` dependency.
- **Impact:** an innocuous shared constant produces browser build failures.
- **Current workaround:** use a browser-safe configuration module and inject
  runtime values. Capture the expected test-layout boundary in tooling or docs.

### Development and policy diagnostics are noisy or misleading

- **Observed in:** both initial apps and deployed-server tests.
- **Surprise:** intentional default-deny omissions produce generic warnings;
  stale built sourcemaps and local schema/catalogue state add unrelated output;
  the default-deny warning is especially misleading while runtime behavior
  disagrees with it.
- **Impact:** important policy failures are harder to distinguish from harmless
  setup noise.
- **Desired outcome:** precise action-level validation and quiet, attributable
  diagnostics.

### Deterministic fixture identities do not map directly to live Jazz row IDs

- **Observed in:** the first BigLabel UI.
- **Surprise:** a fixture identifier such as `org-17-0` cannot be used as the
  generated row ID of a deployed database.
- **Impact:** the UI initially displayed static fixture data while presenting
  itself as connected.
- **Current correction:** discover the current user's live organization through
  membership queries and derive UI state from live rows. We still need a
  reusable fixture-seeding and logical-ID mapping pattern.

### Inline byte columns encourage whole-file buffering

- **Observed in:** BandChat image attachments.
- **Surprise:** the straightforward browser flow calls `arrayBuffer()` for the
  complete file before inserting `s.bytes`.
- **Impact:** an example can accidentally suggest unsafe memory behavior for
  arbitrary attachments.
- **Current mitigation:** BandChat enforces a 256 KiB MIME/size allowlist before
  buffering. Close this item only when large-value streaming has an idiomatic
  documented example or the inline-byte limitation is impossible to miss.

### Browser/native builds expose avoidable setup warnings and cold-build cost

- **Observed in:** BandChat browser builds, BigLabel local-server tests, and
  focused topology/bisect work.
- **Surprise:** expected WASM URL warnings, stale sourcemap warnings, missing
  generated correctness artifacts, and repeated cold NAPI/WASM/RocksDB builds
  dominate feedback for small app changes.
- **Impact:** slower iteration and a poor signal-to-noise ratio for adopters and
  maintainers reproducing app failures.
- **Possible destination:** clearer artifact bootstrap commands, better cache
  reuse, and warnings that distinguish actionable configuration from known
  development-mode behavior.
