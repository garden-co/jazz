# jazz — Specification · 7. Authorization (RLS)

## Overview

jazz authorization is row-level security expressed as queries. Policies describe
which authenticated identities may read or write rows; the fate authority applies
write policy before accepting data, and upstream nodes apply read policy before
shipping data to a peer. This chapter defines the policy model, write
authorization, read narrowing, and policy composition. It builds on queries
(ch. 6) and the transaction/fate machinery (ch. 3).

Invariant digest:

- `INV-API-28`: Db::caninsert, canread, canupdate, and candelete MUST evaluate permissions under the current DbIdentity.author without committing writes, changing local rows, or using...
- `INV-API-29`: A Db is a client: facade writes MUST keep permissionsubject == madeby, and a Db MUST reject any attempt to attribute a write to another author. Cross-author attributio...
- `INV-BRANCH-15`: Branch overlay data MUST NOT ship to a session that cannot read the branch metadata row; branch readability gates overlay visibility before ordinary per-row policy che...
- `INV-RLS-1`: A non-system commit unit MUST be rejected with Fate::Rejected(RejectionReason::AuthorizationDenied) and MUST NOT ingest accepted version rows when any version in the u...
- `INV-RLS-2`: AuthorId::SYSTEM MUST bypass both read and write policy checks.
- `INV-RLS-3`: Policy::owneronly(table, column) MUST compare the named column to claim("sub"), where claim("sub") is bound from the authenticated AuthorId, not from caller-provided q...
- `INV-RLS-4`: A table policy MUST validate as a query shape rooted at the table that carries the policy.
- `INV-RLS-5`: Downstream view emission for a non-system peer MUST only add result members, program facts, and version bundles whose relevant content/deletion versions pass that peer...
- `INV-RLS-6`: Read-policy revocation MUST remove rows from future settled subscription result sets and MUST NOT redact previously delivered local copies from the receiving node.
- `INV-RLS-7`: A deletion-register version by a non-system author MUST satisfy the table write policy against the current global content version for that row; if there is no current...
- `INV-RLS-8`: A deletion-register version MUST be readable to a non-system identity only when the row has a global content winner and that content winner satisfies the table read po...
- `INV-RLS-9`: Join-based policies MUST require at least one matching global-current joined row that reaches the protected row and whose filters pass for the same authenticated ident...
- `INV-RLS-10`: Query-driven sync MUST compose the root table read policy into the subscribed query and bind policy claims from server-authenticated identity so a client cannot widen...
- `INV-RLS-11`: Relay peer links MUST use AuthorId::SYSTEM; edge-client peer links MUST use the terminated client AuthorId for policy-composed reads.
- `INV-RLS-12`: Exclusive transaction view shipping MUST be policy-atomic per recipient and maintained subscription view: a non-system recipient MUST NOT receive a result member or pr...
- `INV-RLS-13`: Historical/as-of reads served for a link MUST evaluate read policy at the requested historical cut.
- `INV-RLS-14`: Policy predicate direct evaluation in the node policy engine MUST treat unsupported predicate/operator forms and unresolved operands as denial, not allowance.
- `INV-RLS-15`: If no read or write policy is declared for a table, the table MUST be public for that operation.
- `INV-RLS-16`: Content extents for large values MUST be visible to an identity only when referenced by a version whose content row passes read policy for that identity.
- `INV-RLS-17`: A write whose Transaction.madeby differs from the authenticated permission subject MUST be accepted only via a trusted serving node (a core/edge Node accepting a Trust...
- `INV-RLS-18`: An uploaded commit unit MUST be authorized under the authenticated link identity: a Session link's madeby MUST equal that identity or be rejected, while a TrustedBacke...

## Details

### 7.1 Policies are shapes

Each table may define a read policy and operation-specific write policies. A
policy is an optional `Query` (ch. 6) over the protected row's columns and the
authenticated claims for the peer being evaluated. The stored core shape is
`read_policy: Option<Query>` plus `write_policies: WritePolicies`, with
`insert_check`, `update_using`, `update_check`, and `delete_using` clauses. If
the relevant policy clause is absent, that operation is **public** for that table
(`TableSchema::new` defaults the read policy and all write clauses to `None`,
`INV-RLS-15`).

An owner-only policy is the canonical single-subject policy: it selects rows
whose ownership column equals the authenticated subject
(`Policy::owner_only(table, column)` is exactly
`Query::from(table).filter(eq(col(column), claim("sub")))`). The `claim("sub")`
operand is the authenticated `AuthorId`, not a caller-supplied parameter
(`INV-RLS-3`). A policy must validate as a shape rooted at the table that carries
it (`INV-RLS-4`), and `AuthorId::SYSTEM` bypasses both read and write checks
(`INV-RLS-2`).

Policy evaluation is **fail-closed**: an unsupported predicate/operator form or
an unresolved operand denies rather than allows (`INV-RLS-14`). Direct policy
evaluation supports equality and inequality, membership/containment predicates,
boolean composition, columns, literals, and authenticated/admission-controlled
claims: `Eq`/`Ne`/`In`/`Contains`/`All`/`Any`/`Not` over column / literal /
`claim(...)`. `claim("sub")` resolves to the authenticated `AuthorId`.
Additional claim names are runtime session claims supplied by the trusted
admission/session layer and must not be client-supplied query bindings. Predicate
forms outside the supported direct-evaluation subset, such as range and null
checks, deny until explicitly supported.

Wire cadence: session claims travel upstream once per connection and again
only when the claim set actually changes; identical re-applications by client
layers must not produce wire messages. The server holds per-connection claim
state, so per-message claims are neither required nor expected.

At the public policy DSL boundary, scalar session-claim checks lower into that
same claim predicate subset. `session.where({ "claims.role": "admin" })` lowers
to claim/literal equality, and `SessionInList { path: ["claims", "role"],
values: [...] }` lowers to a scalar claim membership check equivalent to an
`OR` of claim/literal equality predicates. The core server shell accepts
`session.user_id` / `session.userId` and one-level `session.claims.<name>` paths
for these predicates; deeper claim paths and non-scalar session predicates remain
unsupported at this boundary.

### 7.2 Write authorization

Write policy is an acceptance gate, not a post-acceptance filter. The fate
authority evaluates the relevant operation-specific clause **before acceptance**
for every version in the commit unit. If any version fails, the whole unit is
rejected as
`Fate::Rejected(RejectionReason::AuthorizationDenied)`: it receives no
`global_seq`, makes no durability claim, is audit-only, contributes no accepted
rows, and causes descendants to cascade as described in ch. 3 (`INV-RLS-1`).

For an insert, `insert_check` is evaluated against the inserted row. For an
update, `update_using` is evaluated against the previous content row and
`update_check` is evaluated against the new content row; if both clauses are
present both must pass. For a delete, `delete_using` is evaluated against the row
being deleted. Missing clauses preserve the operation-level public default rather
than falling back to another operation's policy.

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

Branch-scoped writes add a first-level gate before table write policy. The
writer must be able to write the branch metadata row, and the table write policy
is then evaluated inside that branch's overlay-first view (ch. 11,
`INV-BRANCH-15`).

Authorization deliberately separates authorship from permission identity.
`made_by` is the _author_ attribution and is not necessarily the _permission_
identity: a trusted backend (ch. 9, ch. 13) may authenticate as itself while
attributing a mutation to a user. That **attribution-only** case stores user
authorship while evaluating policy against the backend identity. Four identities
are worth keeping distinct:

| identity                            | what it is                                                   | used for                                                            |
| ----------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------- |
| `made_by` (author)                  | who a mutation is _attributed_ to (`Transaction.made_by`)    | provenance (`$createdBy`); _not_ necessarily the permission subject |
| authenticated identity (`AuthorId`) | who a connection authenticated as                            | the subject read/write policies are evaluated against               |
| attribution-only                    | a trusted backend authed as itself but attributing to a user | author ≠ permission identity (ch. 9, ch. 13)                        |
| `AuthorId::SYSTEM`                  | the system identity                                          | bypasses all policies; relay links carry it (§7.3)                  |

At the facade boundary, attributed writes are core-only unless `made_by ==
authenticated identity`. This prevents a client from forging another user's
provenance while still allowing a trusted core backend to evaluate policy as
itself and store user attribution (`INV-RLS-17`, `INV-API-29`).

### 7.3 Read narrowing

Read policy is enforced at the point where data leaves an upstream node. For
each peer identity, the upstream node narrows what it emits before producing any
result-row add/remove, version bundle, rehydrate output, or query update
(`INV-RLS-5`). A relay link carries `AuthorId::SYSTEM` and therefore does not
narrow; an edge-client link narrows under its terminated `AuthorId`
(`INV-RLS-11`, ch. 9).

The security boundary is _upstream emission_, not local storage. Read-policy
revocation removes rows from **future** settled result sets but does **not**
redact a copy already delivered to a receiving node (`INV-RLS-6`). A receiving
node does not re-filter its own local reads or subscriptions by policy. The spec
therefore makes no post-delivery confidentiality promise against a node that
already received data: revocation is forward-looking sync narrowing.

Branch reads use the same first-level gate for branch metadata. A non-system
session may see branch overlay/base data only if it can read the branch metadata
row, and ordinary table read policy is then evaluated inside the branch view
(ch. 11, `INV-BRANCH-15`).

### 7.4 Policy composition for query-driven sync

Query-driven sync must preserve row-level security while evaluating subscribed
shapes. It composes the root table's read policy into the subscribed shape and
**binds the policy's claims from the server-authenticated identity, not from
client-supplied binding values**, so a client cannot widen its visibility by
choosing a different claim binding (`INV-RLS-10`).

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
the read policy for that identity. `INV-RLS-16` — a large-value content extent is
visible to an identity only when referenced by a version whose content row passes
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

Auth-mode gating belongs in permissions rather than process-global flags. A
policy should be able to distinguish anonymous/local/authenticated/backend/system
admission modes through trusted session claims or first-class admission facts;
client-supplied values must not widen those facts.

## Open Questions

### Open questions

- 🔶 **Session/auth model for bindings.** `AuthorId` is currently the runtime
  permission subject and `claim("sub")` value, but the product boundary needs
  explicit account/user/session/default identity terminology. Define how
  anonymous/local sessions, authenticated users, trusted backends, system links,
  and attribution-only writes map to `AuthorId`, claims, and link roles.
- 🔶 **Admission API.** Server and edge shells need an admission hook that turns
  connection credentials into a link identity, claims, role, expiry, and optional
  backend trust. This hook must be the only source for policy claim bindings;
  client-supplied query bindings must never widen claims (ch. 8, ch. 13).
- 🔶 **Admission-controlled claim vocabulary.** `claim("sub")` is normative, and
  arbitrary runtime session claims are supported, but the product boundary still
  needs to define which claims are minted by first-party auth integrations,
  custom admission hooks, trusted backend assertions, and local-only sessions.
- 🔶 **Direct-evaluation predicate expansion.** Direct policy evaluation now
  supports `In` and `Contains` in addition to equality/inequality and boolean
  composition. Range/null predicates remain fail-closed. Decide whether to add
  direct support for the remaining query predicates or reject them earlier in
  policy-specific validation.
- 🔶 **History visibility rule.** Decide whether current-row readability should
  imply visibility for all historical versions of that row, or whether history
  sync/read must evaluate read policy per historical cut.
- 🔶 **Permission subscriptions and TTL.** Edge mergeable authorization uses
  upstream permission-scope subscriptions (ch. 9). The current contract is
  sync-level deduplication and fanout of those scopes; TTL/expiry behavior is a
  future policy for cache lifetime, not a source of permission truth here.
- 🔶 **Write-denial surfacing to clients.** A permission-denied write currently
  never reaches edge durability and `AsyncWriteHandle.wait({ tier })` hangs
  instead of rejecting. Clients need a deterministic rejection signal (analogous
  to `SubscribeRejected` on the read path) so denied writes fail fast. Exposed
  by the auth example denial tests (both auth examples excluded from CI until
  this lands; see `dev/CI_NOTES.md` 2026-07-19).
- 🔶 **Non-claims session references (`session.authMode`).** Policy conversion
  supports only `session.user_id` and `session.claims.*`; the betterauth
  example references `session.authMode`. Decide: promote to a first-class
  session attribute, or migrate such policies to claims.
- 🔶 **String claim validation.** String claim type mismatches in seeded lookups
  should become loud validation errors instead of depending on runtime
  empty-result behavior.
- 🔶 **Uncorrelated policy `EXISTS`.** Server-shell policy conversion currently
  rejects `policy.<table>.exists.where({ userId: session.user_id })` when the
  predicate is used from another table and has no equality against the outer row
  (`__jazz_outer_row`). Decide whether intentionally uncorrelated membership
  checks are valid policy atoms, how to bound them, and how to lower them
  without creating accidental whole-table authority scans. Exposed by
  `world-tour`'s band-member policy.
- ✅ **Permission introspection is a dry-run API, not magic columns.** `$can*`
  columns cannot express _can-insert_ or richer probes; a dry-run is policy
  evaluation _without ingest_ — the write-validation machinery applied
  hypothetically, with local-preview semantics. The facade methods (`can_insert`,
  `can_read`, `can_update`, `can_delete`, ch. 13) are implemented as dry-runs
  (`INV-API-28`).
- 🔶 **Principal authorship migration.** Decide the stable `AuthorId`/principal
  representation for commit authorship, how old self-authored commit encodings
  are rejected or migrated, and where backend attribution helpers are permitted.
- 🔶 **Created/updated provenance magic columns.** `$createdBy`, `$createdAt`,
  `$updatedBy`, and `$updatedAt` need validation, join/filter/order behavior,
  and fail-closed semantics before policy authors rely on them.
- 🔶 **Policy denial reasons.** Policy clauses should be able to return
  structured denial reasons suitable for client errors without exposing data
  from rows the caller cannot read.
- 🔶 **Partial schema visibility.** Decide whether schema/catalogue visibility is
  all-or-nothing per app, scoped by policy, or split into public shape metadata
  plus protected implementation details.
- 🔶 **`NOT(INHERITS)` semantics.** Negative inheritance-style predicates need a
  precise fail-closed meaning before the DSL exposes them.
- 🔶 **Per-column encryption and authorization.** If encrypted columns are added,
  policy evaluation must define what can be evaluated server-side, what requires
  client-side keys, and how key loss/revocation interacts with read policy.
