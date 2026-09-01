# jazz — Specification · 7. Authorization (RLS)

## Overview

jazz authorization is row-level security expressed as queries. Policies describe
which authenticated identities may read or write rows; the fate authority applies
write policy before accepting data, and upstream nodes apply read policy before
shipping data to a peer. This chapter defines the policy model, write
authorization, read narrowing, and policy composition. It builds on queries
(ch. 6) and the transaction/fate machinery (ch. 3).

Invariant digest:

- `INV-API-28`: Permission advice is a three-valued, authority-scoped dry run: only the serving authority may issue definitive Allowed/Denied; client-local, offline, incomplete, not-ready, and timed-out requests yield Unknown. Advice is non-mutating and does not reserve a later mutation; its authenticated request/response exchange exposes no policy evidence and is correlation-, cancellation-, replay-, and dedup-safe.
- `INV-API-29`: A Db is a client: facade writes MUST keep permissionsubject == madeby, and a Db MUST reject any attempt to attribute a write to another author. Cross-author attributio...
- `INV-BVIEW-18`: Read and write policy MUST use ordinary branch columns and the same effective branch view as the operation; missing reference/policy evidence fails closed, and Jazz MUST NOT impose a built-in branch-row existence or lifecycle gate.
- `INV-RLS-1`: A non-system commit unit MUST be rejected with Fate::Rejected(RejectionReason::AuthorizationDenied) and MUST NOT ingest accepted version rows when any version in the u...
- `INV-RLS-2`: AuthorSubject::SYSTEM MUST bypass both read and write policy checks.
- `INV-RLS-3`: Policy::owneronly(table, column) MUST compare the named column to claim("user"), where claim("user") is bound from the authenticated AuthorSubject, not from caller-provided q...
- `INV-RLS-4`: A table policy MUST validate as a query shape rooted at the table that carries the policy.
- `INV-RLS-5`: Downstream view emission for a non-system peer MUST only add result members, program facts, and version bundles whose relevant content/deletion versions pass that peer...
- `INV-RLS-6`: Read-policy revocation MUST remove rows from future settled subscription result sets and MUST NOT redact previously delivered local copies from the receiving node.
- `INV-RLS-7`: A deletion-register version by a non-system author MUST satisfy the table write policy against the current global content version for that row; if there is no current...
- `INV-RLS-8`: A deletion-register version MUST be readable to a non-system identity only when the row has a global content winner and that content winner satisfies the table read po...
- `INV-RLS-9`: Join-based policies MUST require at least one matching global-current joined row that reaches the protected row and whose filters pass for the same authenticated ident...
- `INV-RLS-10`: Query-driven sync MUST compose the root table read policy into the subscribed query and bind policy claims from server-authenticated identity so a client cannot widen...
- `INV-RLS-11`: Relay peer links MUST have an explicit relay transport capability and no permission subject; edge-client peer links MUST use the terminated client AuthorSubject for policy-composed reads.
- `INV-RLS-12`: Exclusive transaction view shipping MUST be policy-atomic per recipient and maintained subscription view: a non-system recipient MUST NOT receive a result member or pr...
- `INV-RLS-13`: Historical/as-of reads served for a link MUST evaluate read policy at the requested historical cut.
- `INV-RLS-14`: Policy evaluation MUST deny when it cannot determine that a policy predicate is satisfied.
- `INV-RLS-15`: A table with no declared policy clauses is public for reads and for writes by non-anonymous permission subjects; anonymous permission subjects are structurally read-only, and once a table declares any clause, every omitted operation is denied.
- `INV-RLS-17`: A write whose Transaction.madeby differs from the authenticated permission subject MUST be accepted only via a trusted serving node (a core/edge Node accepting a Trust...
- `INV-RLS-18`: An uploaded commit unit MUST be authorized under the authenticated link identity: a Session link's madeby MUST equal that identity or be rejected, while a TrustedBacke...
- `INV-RLS-19`: A required include MUST be treated as resolvable for a non-system
  reader only when its target row exists as a current row AND satisfies the target
  table's read policy for that reader; a parent whose required target is missing or
  unreadable MUST be dropped from the result set.
- `INV-RLS-20`: Reads performed to execute a write MUST satisfy the target row's
  read policy; every session-authored update and every upsert of an existing
  target therefore require read permission, while row-id deletes do not.
- `INV-RLS-21`: A policy subplan MUST read its dependency tables as raw policy
  evidence without recursively applying those tables' own read policies, while
  still enforcing the complete outer policy under authenticated claims.
- `INV-RLS-22`: A deletion event's authorization and downstream read eligibility
  MUST resolve its stable physical table lineage back to the logical
  table/schema at the relevant frontier; shared deletion storage MUST NOT widen
  authority across tables.
- `INV-RLS-23`: Jazz derives the reserved logical `session.user` and user
  authorship from the exact trusted JWT subject pair `(iss, sub)`, represented
  portably as canonical JSON `[iss,sub]`. Raw provider claims remain exclusively
  under `session.claims[<name>]`, including `session.claims["iss"]`,
  `session.claims["sub"]`, and a provider claim named `user`.
  Jazz MUST NOT normalize either component, hash the pair into a UUID, or admit
  the reserved system issuer. Local intern handles MUST never become wire,
  storage, query, equality, or ordering values.
- `INV-RLS-24`: Client mutation staging MUST NOT issue a definitive read- or write-policy verdict from partial local state. Update/upsert read visibility and write policy are enforced by the fate authority against its complete admitted policy inputs.

## Details

### 7.1 Policies are shapes

Each table may define a read policy and operation-specific write policies. A
policy is an optional `Query` (ch. 6) over the protected row's columns and the
authenticated claims for the peer being evaluated. The stored core shape is
`read_policy: Option<Query>` plus `write_policies: WritePolicies`, with
`insert_check`, `update_using`, `update_check`, and `delete_using` clauses.

`TableSchema::new` defaults every clause to `None`. Such a **policy-free table**
is public for reads and for writes by non-anonymous permission subjects so an
app can use ordinary data before it introduces authorization. An anonymous
permission subject remains structurally read-only regardless of table policy.
Declaring any one clause closes that table's policy set: the declared operation
is evaluated normally and every other operation with no clause is denied. For
update, either `update_using` or `update_check` declares the update operation;
when both are supplied, both must pass. An absent subclause within an otherwise
declared update contributes no additional check. This rule is enforced by the
fate authority and upstream read emission under the policy-owning schema,
including after schema migration or lens projection (`INV-RLS-15`).

An owner-only policy is the canonical single-subject policy: it selects rows
whose ownership column equals the authenticated subject
(`Policy::owner_only(table, column)` is exactly
`Query::from(table).filter(eq(col(column), claim("user")))`). The
`claim("user")` operand is the canonical authenticated `AuthorSubject`, not
the provider's raw `sub` alone and not a caller-supplied parameter
(`INV-RLS-3`). A policy must validate as a shape rooted at the table that carries
it (`INV-RLS-4`), and `AuthorSubject::SYSTEM` bypasses both read and write checks
(`INV-RLS-2`).

#### Authenticated subjects and provenance

Jazz does not define an application user. The authenticated identity and the
author recorded in `$createdBy` / `$updatedBy` are instead one opaque,
issuer-scoped subject. External JWT authentication retains the exact validated
`iss` and `sub`; self-signed Jazz identities use a reserved Jazz issuer and
their key-derived subject. The portable `AuthorSubject` is the canonical JSON
encoding of the two-string array `[iss,sub]`, with no whitespace or
normalization. The same `sub` from two issuers therefore denotes two authors.

That canonical string is the logical `session.user` value in transactions,
provenance, policy claims, storage, and sync. It does not replace the admitted
provider claims. A provider's `user` claim is `session.claims["user"]` and can
never shadow or spoof `session.user`. Implementations may intern it in memory, but the
intern handle is process-local and has no observable meaning. Provenance
supports equality, inequality, grouping, and equality-index lookup. It is not
orderable: applications sort authors by joining the subject through their own
identity/user rows and ordering an application field such as display name.

The Jazz-owned issuers `urn:jazz:system`, `urn:jazz:local-first`,
`urn:jazz:static-bearer`, and `urn:jazz:anonymous` are reserved. External JWTs
must carry a non-empty issuer and cannot claim any reserved issuer. Internal
admission paths select the latter three only for their named authentication
modes. The distinguished system subject bypasses policy for
internal authority work; it is not a JWT identity and cannot be forged by
supplying claims (`INV-RLS-23`).

### Local-first identity root format

The local-first identity root is outside Groove, row history, sync, and
`AuthorSubject`. It is exactly 32 CSPRNG bytes. Its only portable storage or
export representation is `jazz-auth-v1:` followed by the canonical unpadded
43-character base64url encoding of those bytes. Decoders reject every other
prefix, alphabet, length, padding, and noncanonical form before the value can
reach token minting. English BIP-39 recovery is a direct 24-word/checksum
encoding of the 256 entropy bits (with NFKD/whitespace-normalized input), not
a PBKDF or passphrase-derived seed.

Browser and Expo secure-store adapters share the same codec; plain React
Native must receive an explicit native secure-store adapter. Their logical key
is a versioned hash of byte-exact canonical JSON `{appId, profile}` and never
embeds a raw external subject, PII, or connection/session identifier.
`backendSecret` is a separate deployment admission credential: it is never
persisted by Jazz or Groove, never accepted by client `DbConfig`, and cannot
alter authorship or storage identity. Changing the root creates a new
local-first author. Future root formats use a new prefix and a verified atomic
key-store migration.

Policy evaluation is **fail-closed**: it denies whenever it cannot determine that
a policy predicate is satisfied (`INV-RLS-14`). With the interpreter removed,
this is enforced during policy compilation and claim binding: unsupported
authorization forms compile to no authorized rows in
`NodeState::policy_filtered_current_source_graph_via_query_engine`, and
`NodeState::program_binding_for_shape_and_policy` calls `prepared_claim_value`,
which refuses an unresolved claim rather than binding it as an allowance. The
compiler currently lowers equality and inequality, membership/containment,
boolean composition, columns, literals, and authenticated,
admission-controlled claims: `Eq`/`Ne`/`In`/`Contains`/`All`/`Any`/`Not` over
column / literal / `claim(...)`. `claim("user")` resolves to the authenticated
`AuthorSubject`; raw claim names are supplied by the trusted
admission/session layer and must not be client-supplied query bindings. Predicate
forms the compiler cannot authorize, such as range and null checks, deny until
explicitly supported.

At the public policy DSL boundary, scalar session-claim checks lower into that
same claim predicate subset. `session.where({ "claims.role": "admin" })` lowers
to claim/literal equality, and `SessionInList { path: ["claims", "role"],
values: [...] }` lowers to a scalar claim membership check equivalent to an
`OR` of claim/literal equality predicates. The core server shell accepts
`session.user`, `session.authMode`, and one-level `session.claims["name"]` paths
for these predicates. Flat `session.someClaim` paths and deeper claim paths are
rejected; non-scalar session predicates remain unsupported at this boundary.

### 7.2 Write authorization

Write policy is an acceptance gate, not a post-acceptance filter. The fate
authority evaluates the relevant operation-specific clause **before acceptance**
for every version in the commit unit. If any version fails, the whole unit is
rejected as
`Fate::Rejected(RejectionReason::AuthorizationDenied)`: it receives no
`global_time`, makes no durability claim, is audit-only, contributes no accepted
rows, and causes descendants to cascade as described in ch. 3 (`INV-RLS-1`).

Before table-policy evaluation, the fate authority rejects a commit whose
effective permission subject uses the reserved `urn:jazz:anonymous` issuer.
This structural gate applies to inserts, updates, and deletes received directly
from an anonymous session or relayed by a serving node. It does not change
trusted-backend attribution: `made_by` may record anonymous provenance when the
effective permission subject is the non-anonymous trusted backend.

For an insert, `insert_check` is evaluated against the inserted row. For an
update, `update_using` is evaluated against the previous content row and
`update_check` is evaluated against the new content row; if both clauses are
present both must pass. For a delete, `delete_using` is evaluated against the row
being deleted. Subject to the structural anonymous-write gate, all of those
operations are public on a policy-free table. On a table with any declared
clause, an omitted insert or delete clause denies; an
update with neither `update_using` nor `update_check` denies; and a missing read
policy emits no rows. Missing clauses never fall back to another operation's
policy.

#### Read-for-write authorization

jazz follows PostgreSQL's rule: **reads require read permission, including reads
performed as part of a write** (`INV-RLS-20`). This is not a rule that write
permission implies read permission. The policy unit is the target **row**: jazz
read policies are row-level rather than column-level, so it does not make a
PostgreSQL-style per-column `SELECT` decision.

Every session-authored update MUST be rejected with an authorization error unless
the session may read the target row through its effective view, whether the input
is a partial patch or a full-row replacement and whether it addresses the root
or a branch view. A full replacement must not bypass this rule: identifying and
versioning its target still depends on that target row. Trusted/internal paths
may inspect the authoritative row as policy evidence, but that implementation
privilege does not make a read-hidden row updateable by a session.

The first physical overlay for a branch-view update or existing-target upsert
has no target-row history predecessor even though it read an inherited source.
Every non-root mergeable branch version has mandatory canonical branch-write
intent (ch. 11), so an omitted or relabelled copy cannot be admitted as an
ordinary insert. Its v1 branch-view copy descriptor is the only additional proof that
may satisfy this rule: the authority re-resolves its exact live or frozen source
and evaluates that source's ordinary read policy. The client does not evaluate
or receive policy support, and the descriptor is not a causal dependency,
exclusive read set, or CAS precondition.

An upsert asks whether its target row exists. If there is a current target row,
that is a read and an upsert MUST be rejected unless the writer may read it. If
there is no target row, a table with no read policy may take the insert path; a
table with a row policy MUST deny rather than treating an unreadable target as
absent. Callers that only need to create a row use `insert`. A delete addressed
by row id reads no user data and remains available to a write-only principal,
subject to its delete write policy.

`AuthorSubject::SYSTEM` is reserved here for internal bookkeeping, not for deciding
whether a user read is convenient: causal parent links, index maintenance, and
integrity checks such as `ensure_row_not_deleted` may inspect storage under
system authority. Merging omitted user cells and deciding whether an upsert
target exists are user reads and MUST be evaluated as the writer.

Uploaded commit units are authorized under the **authenticated link identity**,
not under the self-declared `Transaction.made_by`. A normal `Session` link must
upload units whose `made_by` equals that authenticated link identity; otherwise,
the unit is rejected as `AuthorizationDenied`. A `TrustedBackend` link may
upload a unit with `made_by != identity`, but write policy is still evaluated
against the link/backend identity while `made_by` remains provenance
(`INV-RLS-18`; compare the local facade attribution rule, `INV-RLS-17`). A
deletion-register version is authorized against the **current global content
winner** for that row, not against the deletion record; a delete with no current
global content is denied (`INV-RLS-7`).

Partition-relative writes have no built-in metadata gate. Their complete target
tuple is part of the candidate branch-local row, so ordinary table policy may inspect
the bound branch columns or traverse them to application-owned lifecycle and
membership rows. Missing traversal evidence fails closed, and policy is
evaluated in the operation's effective branch view (ch. 11, `INV-BVIEW-18`).

Authorization deliberately separates authorship from permission identity.
`made_by` is the _author_ attribution and is not necessarily the _permission_
identity: a trusted backend (ch. 9, ch. 13) may authenticate as itself while
attributing a mutation to a user. That **attribution-only** case stores user
authorship while evaluating policy against the backend identity. Four identities
are worth keeping distinct:

| identity                                 | what it is                                                   | used for                                                            |
| ---------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------- |
| `made_by` (author)                       | who a mutation is _attributed_ to (`Transaction.made_by`)    | provenance (`$createdBy`); _not_ necessarily the permission subject |
| authenticated identity (`AuthorSubject`) | who a connection authenticated as                            | the subject read/write policies are evaluated against               |
| attribution-only                         | a trusted backend authed as itself but attributing to a user | author ≠ permission identity (ch. 9, ch. 13)                        |
| `AuthorSubject::SYSTEM`                  | a trusted internal system principal                          | bypasses all policies; never implied by relay transport (§7.3)      |

At the facade boundary, attributed writes are core-only unless `made_by ==
authenticated identity`. This prevents a client from forging another user's
provenance while still allowing a trusted core backend to evaluate policy as
itself and store user attribution (`INV-RLS-17`, `INV-API-29`).

### 7.3 Read narrowing

Read policy is enforced at the point where data leaves an upstream node. For
each peer identity, the upstream node narrows what it emits before producing any
result-row add/remove, version bundle, rehydrate output, or query update
(`INV-RLS-5`). Generic, unbound relay traffic has no permission subject and is
not a user query that can be policy-composed. A scope-isolated client relay
instead forwards an upstream
request with a topology-assigned immutable delegated foreground-session binding;
the edge/core authority narrows under that admitted binding, not under SYSTEM
and not under claims supplied by the relay. An edge-client link similarly
narrows under its terminated `AuthorSubject` (`INV-RLS-11`, ch. 9).

Include modes participate in this narrowing rather than sitting outside it. A
required include — an `Include` with `JoinMode::Inner` or `require: true` — counts
as resolvable for a non-system reader only when the target row both exists as a
current row and passes the target table's read policy for that reader. A parent
row whose required target is missing or unreadable is dropped from the result set,
so required-include membership cannot be used as an existence oracle for a row the
reader may not read. Optional and `Holes` includes keep the parent and withhold the
unreadable target instead (`INV-RLS-5`), and `AuthorSubject::SYSTEM` bypasses the policy
half and resolves on existence alone (`INV-RLS-2`, `INV-RLS-19`).

The security boundary is _upstream emission_, not local storage. Read-policy
revocation removes rows from **future** settled result sets but does **not**
redact a copy already delivered to a receiving node (`INV-RLS-6`). A receiving
node does not re-filter its own local reads or subscriptions by policy. The spec
therefore makes no post-delivery confidentiality promise against a node that
already received data: revocation is forward-looking sync narrowing.

A client relay does not become an authority by retaining an authority-selected
result. A scope-isolated relay may publish retained rows and repair payloads to
an exactly same-scope foreground without re-running policy. For Edge/Global
results it must use the exact policy-scoped membership emitted by the authority;
for Local results it uses retained knowledge, including previously delivered
rows that a later authority result removes. A multiplexed relay must keep each
policy binding's authoritative membership separate and may not treat possession
of a cached row as permission to reveal it to another scope (ch. 9,
`INV-EDGE-21..24`).

effective branch-view reads evaluate ordinary table policy over the effective branch-view
view. Partition columns are normal policy-visible values, including references
to application-owned rows that represent a draft or lifecycle when the schema
chooses that model. Jazz has no privileged branch-existence oracle or mandatory
metadata row (ch. 11, `INV-BVIEW-18`).

### 7.4 Policy composition for query-driven sync

Query-driven sync must preserve row-level security while evaluating subscribed
shapes. It composes the root table's read policy into the subscribed shape and
**binds the policy's claims from the server-authenticated identity, not from
client-supplied binding values**, so a client cannot widen its visibility by
choosing a different claim binding (`INV-RLS-10`).

#### Policy dependency authority

An authorization subplan is one complete policy program, not a collection of
user-visible reads. Every table source it inspects is policy evidence and is
read under raw (`System`) authority. Jazz MUST NOT recursively apply a
dependency table's own read policy while evaluating the outer policy. Doing so
would silently change a declared policy `P` into `P AND dependency-policy`, make
the result depend on unrelated policy declarations, and make mutually
referential policy schemas cyclic.

Raw dependency access does not bypass the outer policy. Jazz still evaluates
every filter, join correlation, policy branch, inheritance edge, reachability
seed and step, and other membership constraint declared by that policy. Claims
are bound from the authenticated identity rather than client-supplied values.
Only rows selected by that complete identity-bound program authorize the
protected operation. Failure to compile, bind, or evaluate any required part of
the policy remains fail-closed.

This authority is scoped to policy evaluation. Ordinary queries and includes
against a dependency table still apply that table's own read policy, and policy
evidence does not become independently deliverable payload. Read and write
policy subplans use the same rule.

**INV-RLS-21.** A read or write policy subplan MUST suspend recursive policy
application for all table sources it evaluates and read them as raw policy
evidence. It MUST nevertheless enforce the complete evaluating policy,
including all filters, joins, authenticated claims, policy branches,
inheritance, and reachability constraints. Raw dependency rows MUST NOT become
user-visible merely because they participated in a policy decision, and any
unsupported or indeterminate policy evaluation MUST deny.

Join policies extend that same identity-bound evaluation across relationships. A
join policy passes when a matching global-current row in the joined table reaches
the protected row and its filters hold under the same identity (`INV-RLS-9`).
Policy joins may carry additional source-row equality correlations beyond their
primary join key; these are part of the same join and must be enforced in direct
evaluation, one-shot reads, and maintained subscription views.

Read and write policies are compiled as small boolean programs over policy
atoms. The current atoms include plain column predicates, `reachable_via`, and
`inherits(parent_col)`. Atoms compose with `AND` and `OR`; the composition is
part of the policy program rather than a post-filter outside the query graph.

`reachable_via` supports two seed forms:

- a literal claim value, the degenerate seed used by earlier policies
- a set-valued keyed lookup, written as `seededBy(seed_table, user_col =
claim(path), group_col)`

The set-valued form includes same-table seeds. For example, a team table can
seed reachability by projecting its own `id` column from rows where
`identity_key = claim(sub)`. The seed relation is an ordinary closure input. A
grant, revoke, or seed-column update flows through normal IVM deltas and updates
maintained subscriptions without rehydrating the whole view.

The canonical relation IR for a set-valued seed projects both the seed
membership's group key and each filtered recursive edge's parent key as the
scalar `id` frontier. The recursive step does not join the parent key back to
the seed table: a reachable parent need not have its own seed-membership or
output-table row. Seed and edge predicates are evaluated before their projected
keys enter the frontier.

Reachability deduplicates projected keys at every step, so cycles terminate.
`MaxDepth(N)` is a semantic cutoff: the seed is depth zero, rows reached by
exactly `N` edges are included, and an otherwise valid `N + 1` frontier is
excluded without being treated as evaluator non-convergence. Consequently,
`MaxDepth(0)` is seed-only and MUST NOT admit a one-hop authorization grant.
The evaluator's independent fixpoint safety limit may still report genuine
non-convergence. An empty seed is a live maintained relation, not a completed
subscription; later seed grants, revokes, seed-key moves, filtered edge grants,
and edge revokes produce ordinary maintained deltas.

`inherits(parent_col)` is also an atom. A child row is readable when the parent
row referenced by `parent_col` is readable under the parent's composed read
policy. Missing or invisible parents fail closed. Parent-policy changes
propagate to children through ordinary maintained-view deltas.

Child insert authorization through `inherits(parent_col)` uses parent
updateability evaluated against whereOld only. The parent row is not changed by
inserting the child, so parent whereNew/update-check clauses are not evaluated
for that child insert decision.

`allowedTo.<op>Referencing(sourcePolicy, viaColumn)` is reverse operation
inheritance. It grants access to a target row only when there exists at least one
row in the source table whose `viaColumn` references the target row and that
source row is allowed for the same `<op>` operation. It does not fall back to
source read visibility, insert/update policy, ownership, or mere existence of a
referencing row. For `deleteReferencing`, the source table's `delete_using`
clause is the authority; if no source delete policy exists, enforcing/server
authorization fails closed.

_Further invariants._ `INV-RLS-8` — a deletion-register version is readable to a
non-system identity only when the row has a global content winner that satisfies
the read policy for that identity.
that identity's read policy (ch. 12).

### 7.5 Exclusive atomicity and historical reads

Exclusive transaction view shipping protects recipients from seeing an incomplete
policy-visible fragment for the maintained subscription view. It is
**policy-atomic per recipient and per view**: a non-system recipient receives a
result member or program fact from an exclusive transaction only when every
version required for that view is readable to it (`INV-RLS-12`). Versions
outside that view need not be shipped or readable for the view to advance. This
is distinct from exclusive serializability (ch. 3) and from write authorization:
it governs only read/view shipping.

Historical/as-of reads served for a link evaluate read policy **at the requested
cut**. An ownership change across cuts therefore changes visibility at those
cuts (`INV-RLS-13`, ch. 5, ch. 11).

### 7.9 Subsumed provenance and permission notes

The former principal-authorship TODO is now part of this chapter's backlog:
commit provenance must identify the Jazz principal that performed the write, not
a row object id or raw external provider subject. Creator/updater provenance is
kept as explicit row/version metadata so created-by permissions survive later
updates and history truncation. Public policy helpers such as `$createdBy`,
`$createdAt`, `$updatedBy`, and `$updatedAt` are authorization vocabulary only
after they can be lowered and validated through the same fail-closed policy
machinery as ordinary columns.

Application-specific auth-mode gating belongs in permissions rather than
process-global flags. The structural anonymous-write denial is the exception:
anonymous permission subjects are always read-only, while policies may further
distinguish anonymous/local/authenticated/backend/system admission modes through
trusted session claims or first-class admission facts. Client-supplied values
must not widen those facts.

## Open Questions

- 🔶 [#1758](https://github.com/garden-co/jazz/issues/1758) — Canonical session subject/authorship and provenance.
- 🔶 [#1778](https://github.com/garden-co/jazz/issues/1778) — Admission and binding capability surface.
- 🔶 [#1759](https://github.com/garden-co/jazz/issues/1759) — EXISTS and policy conversion.
- 🔶 [#1760](https://github.com/garden-co/jazz/issues/1760) — Session-claim policy conversion.
- 🔶 [#1761](https://github.com/garden-co/jazz/issues/1761) — Relational read-policy grants.
- 🔶 [#1762](https://github.com/garden-co/jazz/issues/1762) — Write authorization for read-hidden and inherited rows.
- 🔶 [#1763](https://github.com/garden-co/jazz/issues/1763) — Bounded, cycle-safe policy graphs.
- 🔶 [#1779](https://github.com/garden-co/jazz/issues/1779) — Policy replacement across schema evolution.

### Detailed issue context

- **Session/auth model for bindings.** `AuthorSubject` is the runtime
  permission subject and reserved `claim("user")` value, but the product boundary needs
  explicit account/user/session/default identity terminology. Define how
  anonymous/local sessions, authenticated users, trusted backends, system links,
  and attribution-only writes map to `AuthorSubject`, claims, and link roles.
- **Admission API.** Server and edge shells need an admission hook that turns
  connection credentials into a link identity, claims, role, expiry, and optional
  backend trust. This hook must be the only source for policy claim bindings;
  client-supplied query bindings must never widen claims (ch. 8, ch. 13).
- **Admission-controlled claim vocabulary.** `claim("user")` is reserved, and
  arbitrary runtime session claims are supported, but the product boundary still
  needs to define which claims are minted by first-party auth integrations,
  custom admission hooks, trusted backend assertions, and local-only sessions.
- **Direct-evaluation predicate expansion.** Direct policy evaluation now
  supports `In` and `Contains` in addition to equality/inequality and boolean
  composition. Range/null predicates remain fail-closed. Decide whether to add
  direct support for the remaining query predicates or reject them earlier in
  policy-specific validation.
- **Policy replacement across schema evolution.** The current implementation
  selects the active policy-owning schema independently for each operation: a
  newer schema replaces a read, insert, update, or delete policy only when it
  declares the applicable clause; otherwise that operation can continue using
  the preceding active policy definition. Decide whether this is the intended
  migration model, or whether every newly activated schema must instead provide
  a complete replacement policy bundle for every surviving table. A
  policy-complete model must define whether an omitted clause means public,
  inherited, or invalid, and catalogue validation must reject ambiguous partial
  replacements. It must also define what replacement means when a table is
  renamed, split, copied, or dropped: whether the old table's policy disappears,
  remains available only for historical/old-schema operations, or must be
  explicitly mapped or tombstoned by the lineage publication. The decision must
  preserve deterministic authorization for old authored versions, live clients
  on older schemas, historical reads, and operation-specific permission advice.
- **History visibility rule.** Decide whether current-row readability should
  imply visibility for all historical versions of that row, or whether history
  sync/read must evaluate read policy per historical cut.
- **Permission subscriptions and TTL.** Edge mergeable authorization uses
  upstream permission-scope subscriptions (ch. 9). The current contract is
  sync-level deduplication and fanout of those scopes; TTL/expiry behavior is a
  future policy for cache lifetime, not a source of permission truth here.
- **Write-denial surfacing to clients.** A permission-denied write currently
  never reaches edge durability and `AsyncWriteHandle.wait({ tier })` hangs
  instead of rejecting. Clients need a deterministic rejection signal (analogous
  to `SubscribeRejected` on the read path) so denied writes fail fast. Exposed
  by the auth example denial tests (both auth examples excluded from CI until
  this lands; see `dev/CI_NOTES.md` 2026-07-19).
- ✅ **Session references are explicit.** Policy conversion supports the canonical
  `session.user`, the reserved `session.authMode`, and raw provider values only
  under `session.claims.*`. Former flat identity aliases are not retained.
- **String claim validation.** String claim type mismatches in seeded lookups
  should become loud validation errors instead of depending on runtime
  empty-result behavior.
- **Uncorrelated policy `EXISTS`.** Server-shell policy conversion currently
  rejects an uncorrelated membership predicate when the
  predicate is used from another table and has no equality against the outer row
  (`__jazz_outer_row`). Decide whether intentionally uncorrelated membership
  checks are valid policy atoms, how to bound them, and how to lower them
  without creating accidental whole-table authority scans. Exposed by
  `world-tour`'s band-member policy.
- ✅ **Permission introspection is an authority dry-run API, not magic
  columns.** `$can*` columns cannot express _can-insert_ or richer probes. The
  facade methods (`can_insert`, `can_read`, `can_update`, `can_delete`, ch. 13)
  produce `Allowed`, `Denied`, or `Unknown`; only the serving authority may
  issue a definitive result. A local, offline, incomplete, not-ready, or timed
  out client receives `Unknown`, never a local policy decision. Requests are
  evaluated under the authenticated link identity and return only an opaque
  correlation id plus the advice value, never supporting rows, policy reasons,
  or hidden dependency facts. Advice is non-mutating and does not reserve or
  authorize the ordinary optimistic write that may follow (`INV-API-28`).
- **Safe local permission fail-fast.** A future client-local `Denied` may be
  added only when it is mechanically proven that every fact required for that
  rejection is locally complete (for example, proposed-row or structural facts).
  Missing policy support is never denial proof. Local `Allowed` remains
  forbidden without the serving authority.
- **Policy denial reasons.** Policy clauses should be able to return
  structured denial reasons suitable for client errors without exposing data
  from rows the caller cannot read.
- **Partial schema visibility.** Decide whether schema/catalogue visibility is
  all-or-nothing per app, scoped by policy, or split into public shape metadata
  plus protected implementation details.
- **`NOT(INHERITS)` semantics.** Negative inheritance-style predicates need a
  precise fail-closed meaning before the DSL exposes them.
- **Per-column encryption and authorization.** If encrypted columns are added,
  policy evaluation must define what can be evaluated server-side, what requires
  client-side keys, and how key loss/revocation interacts with read policy.
