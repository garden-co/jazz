# CORRECTNESS BURNDOWN

This is the durable inbox for known correctness, security, lifecycle, and
architecture-boundary risks. `TEST_BURNDOWN.md` tracks quarantined executable
tests; this document tracks findings that may already have a local fix, may not
yet have a faithful regression, or require an explicit design decision.

## Lifecycle

Every entry has a stable ID, severity, status, evidence, and exit criterion.
New whole-repository reviews add findings here before their lane is retired.

An entry may be removed only after one of these outcomes lands on `main`:

- a fix plus a mutation-sensitive regression;
- an executable invariant or gate that makes the failure unrepresentable; or
- an explicit specification decision that accepts and precisely bounds the
  behavior.

Moving code, closing a PR, or observing one green run is not sufficient. Local
commits and open PRs remain tracked until merged.

Statuses are `investigating`, `fix in progress`, `fixed locally`, `in review`,
and `blocked on design`.

## Active findings

| ID       | Severity | Status            | Finding and evidence                                                                                                                                                                                                                                                                     | Current work                                                                                                                                                                                                                             | Exit criterion                                                                                                                                                                                                                                                        |
| -------- | -------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CB-001` | High     | fixed locally     | Unsubscribing a subscription whose initial hydration is suspended on storage removed only its public state. The private evaluation and storage future remained pending, so later progress could block on abandoned work.                                                                 | `e863bb05a` on `audit/core-correctness-security` cancels suspended hydration and releases temporal successors.                                                                                                                           | Land the fix and both planted-negative-sensitive regressions: cancellation without granting the blocked storage permit, and successor progress after cancelling the first shared cold hydration.                                                                      |
| `CB-002` | High     | fixed locally     | Async relation snapshots and subscription opening used legacy try-lock access. A concurrent cold operation holding the node mutex caused a panic instead of backpressure.                                                                                                                | `c30adecac` on `audit/core-correctness-security` awaits the node lock throughout async relation reads and subscription setup.                                                                                                            | Land the fix and black-box cold-storage concurrency regression; retain the planted legacy-lock failure receipt in review notes.                                                                                                                                       |
| `CB-003` | High     | fix in progress   | `SubscriptionStream` drop-time cleanup synchronously borrows the async node. Dropping during a cold node operation can panic. Local and upstream cleanup paths share this lifecycle risk.                                                                                                | Dedicated async-audit lane is implementing a node-owned idempotent cleanup queue plus `close().await` acknowledgement.                                                                                                                   | `Drop` never blocks, spawns, leaks, or acquires the async node lock; local and upstream cleanup drain under normal node ownership; explicit close is deterministic and cancellation-safe; contention, repetition, stale-runtime, and shutdown cases have regressions. |
| `CB-004` | High     | fixed locally     | Runtime default-allow behavior for omitted policy actions disagreed with validator warnings and made partially protected tables open to undeclared mutations. Closing partial policy sets also exposed that current-row write evidence incorrectly depended on ordinary read permission. | `7aa36558c` on `fix/closed-policy-set` makes policy-free tables open, closes omitted actions once any policy exists, and compiles current-row write evidence as a raw authorization subplan.                                             | Land aligned core, edge, migration, cache/subscription, validator, documentation, and deployed authority receipts, including the planted missing-insert regression.                                                                                                   |
| `CB-005` | Medium   | fixed locally     | Generic Groove regressions accidentally depended on RocksDB, creating a reverse dev-dependency and duplicate Groove trait instances. Some Memory restart tests also reopened a fresh empty store rather than the store under test.                                                       | `a6070b6d7` on `cleanup/remove-opfs-btree` ports generic receipts to Memory, preserves storage across restart, and moves the genuine disk/reopen receipt to `jazz-storage-rocksdb`.                                                      | Land the OPFS removal after independent review with focused storage, restart, IVM, and RocksDB receipts green. Preserve the known long recursive workload in the test burndown rather than silently dropping it.                                                      |
| `CB-006` | High     | in review         | BigLabel initially allowed tenant self-admission, role/assignment forgery, and foreign mutations because mutation policies were omitted and deployed runtime behavior was permissive. Its synthetic fixture test could not observe an authority breach.                                  | `feat/big-label` at `9e6600884` adds explicit mutation policies and a deployed local-authority regression for admission, escalation, assignment forgery, foreign reads, and foreign writes. Core prevention is also covered by `CB-004`. | Independently plant a policy regression against the final branch, land the deployed authority receipts, and ensure the example remains secure under the final closed-policy-set semantics.                                                                            |
| `CB-007` | Medium   | blocked on design | Legacy synchronous facade calls such as `read`, `prepare_query`, query attachment, and some lifecycle cleanup use try-lock or blocking access and can panic when a cold async operation owns the node. Blindly awaiting would change synchronous API contracts.                          | No implementation dispatched beyond the subscription-finalization subset in `CB-003`.                                                                                                                                                    | Specify which APIs remain synchronous, their concurrency contract, and the ownership/actor boundary; then add black-box contention receipts and either serialize safely or return a typed busy/error outcome without panic.                                           |
| `CB-008` | Medium   | investigating     | The standalone React Native binding imports fourteen APIs removed by the async core. Repairing it would preserve an obsolete parallel path and its current package-contract tests verify source shape rather than a working native artifact.                                             | `feat/restore-react-native` establishes SQLite storage, a per-auth native relay, Expo packaging contracts, and honest scaffold status on current `main`.                                                                                 | Replace the obsolete binding through the relay command/codec boundary and prove real XCFramework/AAR artifacts, autolinking, a device app, and Blacksmith device receipts before claiming RN support.                                                                 |

## Recently contained example-app findings

These stay here until their PRs merge because the examples are intended to be
copyable best practice, not merely demonstrations.

| ID       | Severity | Status    | Finding and evidence                                                                                                                                     | Current work                                                                                                                                                                        | Exit criterion                                                                                                                           |
| -------- | -------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `CB-009` | High     | in review | BandChat originally allowed room self-admission and forged message authorship; its browser tests did not deploy policy or prove reconnect delivery.      | PR #1714 binds membership and authorship, adds deployed edge receipts, and proves offline write → reconnect → fresh-store delivery with the canonical controllable browser harness. | Merge the mutation-sensitive authority and topology receipts.                                                                            |
| `CB-010` | Medium   | in review | BandChat's first pass loaded a broad graph at the app root and mixed provisioning into the initial read path, making the example a poor pattern to copy. | PR #1714 head `c12c83688` uses narrow query-owning components and a separate explicit, idempotent provisioning path.                                                                | Merge with component-level queries, side-effect-free reads, provisioning idempotence coverage, and the existing topology receipts green. |

## Review intake

Whole-repository review lanes should prioritize and record findings from these
boundaries:

- authorization, validation, and policy-evidence trust boundaries;
- async suspension, cancellation, retry, shutdown, and handoff;
- storage durability, reopen, migration, and cache invalidation;
- subscription staleness, duplicate delivery, and lifecycle ownership;
- specification/invariant drift and tests insensitive to planted regressions;
- duplicated architectural paths, especially across web, N-API, and native
  bindings;
- panics, unbounded work or memory, and attacker-controlled protocol inputs.

DX and build papercuts belong in the relevant engineering-learning log unless
they can hide a correctness failure, invalidate a receipt, or prevent a gate
from running reliably.
