# Lens Hardening

## What

Harden Jazz lens semantics and tooling so schema evolution stays deterministic, reviewable, and safe under mixed-version traffic. This includes making lens-path selection ambiguity-aware, supporting corrected or asymmetric migrations for the same schema pair, and defining an explicit story for type-changing migrations.

## Notes

- Lenses are what make overlapping schema versions workable. If the edge cases are loose, rollouts can turn into silent data loss, nondeterministic behavior, or migrations that cannot be corrected once published.

Deterministic path selection:
Equal-hop lens paths can encode different semantics, for example a diamond graph where both 2-hop paths reach the same target but inject different defaults or rename through different intermediate shapes. Path choice be deterministic. Even if we reject ambiguous shortest paths, that needs to be a deliberate invariant of the lens model.

Lens revisions and asymmetric behavior:
Lens identity is currently tied to `(source_hash, target_hash)`. That makes it hard to publish a corrected migration for the same schema pair, and it also does not leave room for intentionally asymmetric backward behavior to propagate cleanly. We need a revision or identity story for "same schemas, better migration."

Type-changing migrations:
Type changes are currently surfaced as ambiguities rather than executable transforms. That is safer than auto-casting, but it means `Text -> Integer`, `Json -> Text`, enum reshapes, and similar changes do not yet have a first-class migration model. We should decide whether the answer is richer lens ops or a different migration strategy (which we'd need to document).
