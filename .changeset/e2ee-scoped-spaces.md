---
"jazz-tools": patch
---

Add explicitly configured scoped spaces, atomic account/group initial recipients,
signed grants and rotations, epoch history, independent recovery, permanently sealed
lineages, and interrupted initial delivery resumption. Deterministic scope-bound root
IDs and exclusive exact-row preconditions prevent competing or hidden roots. Reads
require live authority settlement; encrypted ordinary operations and durable offline
accepted-history persistence are separate layers.

Expose `spaceSchema` through `jazz-tools/e2ee` for application composition, and use
the consolidated recovery decoder for space status. Replay checks each root's
deterministic ID. Recovery status and device reads propagate full-history failures
rather than reporting them as unavailable deliveries.
Use the current global query tier for space authority observations.

Space operations share session-owned device keys and synchronous private-store
decoding with the account and group lifecycles. Preserve recovered-key tuples
and unrelated store fields, and support optional space tables in schema-only
application configuration.

Resolve space group recipients through explicit historical scopes, including
groups outside the initializer's memberships. Preserve account/group ambiguity
checks and complete accepted-history validation without application-wide group
discovery.

Keep a space sealed when its initial recipient group empties before a later
grant. Treat synchronous and asynchronous candidate-envelope failures alike,
without hiding failures in confirmed predecessor history. Protected recovery
can try another root after space-owned delivery exhaustion; operational errors
and public diagnostic-code collisions still abort.

Discover recovery obligations from settled direct and inherited recipient grants
in bounded ID batches, not all application roots. Isolate canonical roots from
wrong-ID proposals during creation and lookup, and exclude covered ineligible
authors without hiding authority or verifier failures. Reject incomplete managed
group configuration while preserving account-only spaces.

Index table-qualified settlement positions and capture space replay authority
once per covered snapshot, avoiding repeated delivery-byte comparisons.
Preserve cutoff-specific validation and live eligibility checks, and prevent
adapter-mutated space or group replay from contaminating later cached proofs.
