# Shared physical scope inputs — replacement experiment for #2952

Authorized by Anselm as a sibling alternative to #2952, with common PR base
`8021dffb31628a3f7f97ede47c30a421354aa13d` (#2951). Preserve #2952 and its
receipts; no merge or adopter deployment until explicitly approved.

## Thesis

The wire already sends one physical row/version set per exact authority scope.
Receiver normalization currently broadcasts each row to every compiled table
occurrence, retaining occurrence-sized facts and separately materialized IVM
inputs. This is receiver plumbing, not information supplied by the sender.
Replace that expansion with shared scope/table membership and shared input
datasets. Aliases, filters, schema projections, branch semantics and local
overlays remain explicit downstream graph operations. Never share authority
membership across different query/identity/read-view scopes.

The target covers the entire sender -> wire -> receiver chain, not just receiver
allocation. Sender support maintenance should yield one physical membership
frontier and its net deltas directly, without a second occurrence-fact-to-scope
aggregation in publication. The receiver should consume that frontier directly,
not translate it back into occurrence-shaped facts (including fake Root roles).
Query/policy proof dependencies remain internal to evaluation; they are not a
second sync membership representation.

Exact equivalence to #2952's v2 wire is NOT an acceptance constraint. Its physical
snapshot/delta vocabulary looks compatible with this model, but may change if
end-to-end simplification requires it. Preserve exact authority scope, atomic
publication, predecessor/recovery and missing-body correctness, not incidental
layout. Any wire or durable-state encoding change must be explicitly specified,
versioned appropriately and byte-pinned; serializer defaults are not contracts.

## Changed premise and preflight

Read the rejected-experiment ledger and Rust testing guidelines. Searched
preserved receiver/source/scope branches and open/closed receiver PR bodies.
#2927/#2935 optimize occurrence expansion, but retain its multiplicity. The
same-drain shared-membership fix on `fix/covered-source-shared-membership`
protects aggregate sender justifications and must remain intact. Rejected
#2830 only optimizes witness metadata encoding; #2848 globally shares record
ownership. This trial removes the receiver occurrence boundary instead of
repeating either mechanism.

## Acceptance and comparisons

Compare against both the common parent and #2952 with identical native todo
insert/update, batch/reopen, and permissioned cold-load fixtures; seal source
and binary receipts and confirm with CodSpeed/full GQL profiles. Prediction:
retain #2952's ~15% insert / ~27% update gain, recover its ~1% cold penalty and
look for additional cold benefit from less representation duplication. No
specific larger cold win is established yet.

Compare production/test diff size, retained scope indexes, normalization paths,
and per-physical-row versus per-occurrence materializations, not just raw LOC.
Also compare sender membership/reference-count layers and publication adapters;
a receiver-only simplification is an intermediate checkpoint, not completion.
Tests must cover self-joins/repeated paths, permissions, lens/branch boundaries,
pending local writes, scope retirement/reconnect, and atomic missing-body repair.
An internal work-bound test is justified only for input/source allocation and
materialization counts that cannot be observed through public query equality.

Theses, results and unresolved follow-ups remain in GitHub issue #2913.

## Whole-chain implementation

Sender terminals now update a single weighted physical-row frontier and a
touched-row journal. Publication consumes its net delta; it no longer maintains
a second full published fact set or a second physical reference-count map.
Acknowledged membership can be recovered from the current frontier plus the
journal, so failed/cancelled bundle construction does not lose its predecessor.

The v2 physical Snapshot/Delta vocabulary remains because it already expresses
these inputs, not because byte equivalence constrained the design. The receiver
retains physical coordinates/versions directly, with one shared table input
beneath occurrence projections and local overlays. It does not fabricate source
coverage facts or Root-role carriers. Inputs prepared before initial catalogue
adoption are rebound to trusted physical IDs at the snapshot boundary.

Durable scope rows use the explicit, byte-pinned JSIR v1 codec. As approved by
Anselm, old JPFK occurrence caches and resume cursors are discarded, not migrated.
Native rows, transaction history, pending writes and catalogue data survive.
Tests for the old internal representation now exercise physical frames; the
historical corpus still checks every primary byte while allowing the authorized
derived-cache invalidation. A dedicated recovery test checks both data retention
and fresh subscription recovery. Offline permissioned settlement cannot reuse
the discarded authority proof.
