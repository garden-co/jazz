# Shared selective query hydration

Experiment and outcome authority: [#2913](https://github.com/garden-co/jazz/issues/2913).
Baseline: [#3072](https://github.com/garden-co/jazz/pull/3072), policy-scoped
documents fixture revision 1. Its query-only cases remain unchanged.

The execution contract is one incrementally correct current-query graph:
one-shot readers await initial settled hydration and release their handle;
subscriptions retain it for subsequent changes. A request-derived index prefix
restricts candidates, not authorization semantics. Every protected-root policy
alternative sees the same candidates, while supporting source occurrences
retain their own semantics. A per-binding prefix must not enter a reusable
identity-neutral policy cache.

Measure both query-only first reads and subscription initial results. Independent
result oracles must also check future inserts into an empty prefix, membership
changes, deletion/restore, and permission grant/revoke. Sharing execution means
one-shot-versus-subscription comparisons alone cannot prove correctness.

This does not introduce an ordered compound index or promise page-sized work.
Local reads stay local; remote reads retain fresh coverage gates. One-shot
release must not retire another live owner's graph. Storage and wire encodings
are unchanged by this design.

The consumer still determines its required outputs. A first-result reader
requests application rows, not authority sync-witness terminals; the native
`CurrentRow` adapter consumes the compiler's materialization layout to preserve
provenance and joined-row identities. Retained consumers keep their existing
terminal contracts. Both use the same graph installation and binding lifecycle;
profiled current reads use that same implementation, with opt-in phase clocks.

The existing policy-scoped primary-key deletion safeguard from
[#2187](https://github.com/garden-co/jazz/pull/2187) remains for retained
subscriptions. Initial-result consumers retain their proven point-read path
and release it before subsequent writes. This experiment does not claim to
remove that retained-lifetime restriction. Secondary equality candidates use
live index sources for both lifetimes, without snapshot-only intersections or
source limits.
