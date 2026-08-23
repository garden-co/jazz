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

| ID       | Severity | Status            | Finding and evidence                                                                                                                                                                                                                                            | Current work                                                                                                                                                                                                                                                    | Exit criterion                                                                                                                                                                                                              |
| -------- | -------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CB-006` | High     | fix in progress   | BigLabel initially allowed tenant self-admission, role/assignment forgery, and foreign mutations because mutation policies were omitted and deployed runtime behavior was permissive. Its synthetic fixture test could not observe an authority breach.         | Draft PR #1719 (`feat/big-label` at `ced957707`) adds explicit mutation policies and deployed local-authority regressions for admission, escalation, assignment forgery, foreign reads, and foreign writes. The core partial-policy prevention landed in #1718. | Merge #1719 after its independently reviewed, mutation-sensitive authority receipts and normal CI are green; the example must remain secure under the landed closed-policy-set semantics.                                   |
| `CB-007` | Medium   | blocked on design | Legacy synchronous facade calls such as `read`, `prepare_query`, query attachment, and some lifecycle cleanup use try-lock or blocking access and can panic when a cold async operation owns the node. Blindly awaiting would change synchronous API contracts. | The subscription-finalization subset landed with #1721; the remaining synchronous facade contract has no implementation branch.                                                                                                                                 | Specify which APIs remain synchronous, their concurrency contract, and the ownership/actor boundary; then add black-box contention receipts and either serialize safely or return a typed busy/error outcome without panic. |
| `CB-008` | Medium   | fix in progress   | The standalone React Native binding imports fourteen APIs removed by the async core. Repairing it would preserve an obsolete parallel path and its current package-contract tests verify source shape rather than a working native artifact.                    | Draft PR #1720 (`feat/restore-react-native` at `57e4a55c9`) establishes SQLite storage, a per-auth native relay, Expo packaging contracts, and honest scaffold status on current `main`.                                                                        | Replace the obsolete binding through the relay command/codec boundary and prove real XCFramework/AAR artifacts, autolinking, a device app, and Blacksmith device receipts before claiming RN support.                       |

## Resolved findings

These entries met their exit criteria and were removed from the active inbox.
The listed commits are ancestors of `main`; the original regressions remain in
the tree.

| ID       | Resolution                                                                                                                                                                        |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CB-001` | #1721 landed `f00c6c0fc`, identical to the original cancellation fix `e863bb05a`, with the cold-cancellation and shared-successor receipts.                                       |
| `CB-002` | #1721 landed `f01c8e93c`, identical to `c30adecac`, with the cold concurrent relation-snapshot/subscription receipt.                                                              |
| `CB-003` | #1721 landed queued, terminal subscription finalization (`418322371`, `9a9963ecc`, and `f0cd13e97`) with contention, terminal-close, shutdown, and ordinary-owner drain coverage. |
| `CB-004` | #1718 landed the closed partial-policy-set implementation and its core, authority, validator, migration/cache, and planted-missing-insert receipts.                               |
| `CB-005` | #1717 landed OPFS removal, Memory-backed generic storage receipts, and the dedicated RocksDB reopen coverage.                                                                     |

## Recently contained example-app findings

These stay here until their PRs merge because the examples are intended to be
copyable best practice, not merely demonstrations.

| ID       | Severity | Status          | Finding and evidence                                                                                                                                     | Current work                                                                                                                                                                                            | Exit criterion                                                                                                                           |
| -------- | -------- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `CB-009` | High     | fix in progress | BandChat originally allowed room self-admission and forged message authorship; its browser tests did not deploy policy or prove reconnect delivery.      | Open PR #1714 at `7e2e3a5e7` binds membership and authorship, adds deployed edge receipts, and proves offline write → reconnect → fresh-store delivery with the canonical controllable browser harness. | Merge the mutation-sensitive authority and topology receipts with CI green.                                                              |
| `CB-010` | Medium   | fix in progress | BandChat's first pass loaded a broad graph at the app root and mixed provisioning into the initial read path, making the example a poor pattern to copy. | Open PR #1714 at `7e2e3a5e7` uses narrow query-owning components and a separate explicit, idempotent provisioning path.                                                                                 | Merge with component-level queries, side-effect-free reads, provisioning idempotence coverage, and the existing topology receipts green. |

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
