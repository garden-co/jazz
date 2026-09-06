# Account admission and identity linking

## Overview

Implementation contract for #2611. Accounts are stable application-scoped
ownership identities. Principals remain exact `(iss, sub)` pairs and retain
provenance. Authenticating a principal does not register it. Public application
contexts require an opaque AccountHandle created by the account helpers.

The Rust core owns registry transitions and verifies admission. Shared
TypeScript owns credential refresh, account selection, and observable state.
Environment adapters own credential persistence. Framework adapters subscribe
to the shared state; they do not implement independent authentication flows.

## Registry invariants

- Within one application registry a principal can be assigned to at most one
  account, permanently. Revocation never deletes or reassigns that association.
- Active admission and permission to manage identities are distinct from the
  permanent assignment. Founders initially have both permissions.
- External registration is explicit and ordered. Login only resolves an
  existing active assignment.
- Local-first founding is deterministic from the application namespace and
  verified founding key identity. It is usable offline; arbitrary external
  assignment cannot be accepted offline.
- A linking request records the destination account, approving principal,
  approving admission generation, exact candidate principal, random nonce,
  and expiration. Acceptance authenticates the candidate independently.
- Acceptance atomically checks the approver is still admitted with management
  authority in the recorded generation, reserves the candidate's assignment,
  and records consumption. Competing claims cannot both succeed.
- A completed nonce is idempotent, but replay cannot reactivate a revoked
  identity. Revocation/reinstatement cannot revive old pending approvals.
- A successful response follows durable commit. Storage errors cannot publish
  speculative admission in memory. Authority restart preserves assignments,
  revocations, and replay outcomes.
- Edge nodes cannot independently accept registry mutations. Routing must
  preserve the application's single authoritative decision order.
- Provider JWT claims never supply an authoritative account ID. Existing JWT
  verification supplies a principal; the registry supplies its account.

## Public API

An application-scoped account manager provides:

- `createLocalFirst(): AccountHandle`: synchronous offline founding and selection.
- `getLoggedIn(): AccountHandle | undefined`: local selected state, not a fresh
  remote admission guarantee.
- `registerJWT(auth): Promise<AccountHandle>`: explicit registration and selection.
- `loginJWT(auth): Promise<AccountHandle>`: existing active assignment and selection.
- `linkJWT(auth): Promise<AccountHandle>`: link to the selected account and select
  a new handle acting as the linked principal.
- `logout()`: clear selection and stop credential use without remote revocation
  or silently deleting recoverable local-first credentials.

Async failures reject with typed errors and preserve the previous selection.
Credential inputs support refresh; plain JWT strings are a convenience.
Concurrent operations cannot restore a login after logout or overwrite a newer
selection. Successful remote side effects remain discoverable through login
when the initiating UI operation was canceled.

Handles are opaque, immutable, application-scoped objects. They expose account
ID and acting principal, not secrets. Existing contexts remain bound to their
handle; selection changes require a new context. Runtime checks complement TS
branding. A handle is not a substitute for server-side authorization.

## Framework state

Shared observable state distinguishes restoring, logged out, ready, and pending
operations. A pending login/link preserves the current handle. Operation errors
are observable without destroying a working context. Framework providers own
context lifecycle and dispose the old context when deliberately switching,
without treating successful linking as proof that pending writes synchronized.
SSR state is request-scoped. Native secure storage and browser persistence are
adapters, not separate implementations of the state machine.

## Ownership, storage, and wire

Account ownership and exact principal provenance must both survive writes,
replication, and reopening. Equality must be explicit and consistent across
policies, joins, indexes, and JS; no partially comparing principal tuple.
`$createdBy` and `$updatedBy` are structured author values:
`{ account, identity: { issuer, subject } }`. Normal account contexts supply an
account ID; internal principals without an account expose a null account.
Rust interns the entire author structure, including account and both principal
fields. Intern handles are process-local implementation details and never enter
storage or wire encodings. Account ownership policies compare `.account`;
whole-author equality compares every field, so two linked identities acting for
one account remain distinct authors. JavaScript object reference identity is
not a portable equality contract.
The final PR must document exact before/after ownership representation and all
new durable and wire codecs before review. This work precedes storage freeze.

Linking changes authorization; it does not migrate data or merge accounts.
Pending local writes remain recoverable until synchronized. Account handles
must not open storage belonging to another account, and remote revocation must
have defined effects on admission, ongoing subscriptions, and writes.

## Acceptance

Exercise public APIs for offline founding/reopen; explicit registration;
unassigned login rejection; signup/link/reload on a second device; exact-issuer
separation; competing claims; expiration; cancellation; approver revocation;
nonce replay after revocation; durable restart; storage failure; and unrelated
account denial. Prove test sensitivity with meaningful planted negatives.

Cover browser, Node, and RN bindings plus React, Svelte, Vue, and Solid state
adapters. Update examples, all starter variants, and auth documentation to use
handles. Retain low-level principal APIs only where explicitly appropriate for
server authentication and internal tests, not as an application context bypass.

## Open Questions

Implementation details and remaining work are tracked in #2611. The PR must
resolve ownership representation, durable registry integration, and remote
revocation semantics before it is considered ready.

## Revocation ordering

Revocation is an admission boundary, not an erase or synchronous drain barrier.
Registry login and revocation are serialized by the core authority. A bounded
operation admitted before revocation may finish, including an already-admitted
write or subscription delivery. Every later inbound operation and outbound
subscription tick must recheck admission; idle sessions observe registry changes
and close. Trusted backend/admin service connections use their separate
authority and are not public account sessions.

Edges authenticate the original JWT before resolving admission against the core.
A service-authenticated registry lookup is read-only: it cannot register, found,
link, or revoke an identity. Local-first founding forwarded by an edge requires
the founder's original bearer proof. Edges recheck admission for each operation
and delivery, fail closed if the core is unavailable, and poll idle sessions
once per second until a registry watch protocol replaces that polling. Public
account mutation forwarding never substitutes an edge service credential.

## Context lifecycle and linking

An account's enrollment authority is independent of its context's active
transport. Omitted context `serverUrl` means local-only; it must not be filled
implicitly from the handle. A supplied URL must match the handle's registry.
The handle-derived registry authority participates in the durable account
namespace even without transport: registry, application, environment, and
account identify the root. Exact acting identity additionally partitions live
sessions. Two registries assigning identical account UUIDs must never alias
local storage. Applications cannot supply or override this internal authority
field separately from the opaque handle.

Enrollment has no dependency on an open database or a framework. `linkJWT`
operates on the selected handle's credential and the core registry only; it
never enumerates contexts, drains uploads, or rewrites transaction authors.

The application or local-first auth helper uses the general lifecycle sequence:

1. Stop admitting application work and call `oldContext.shutdown({ waitForSync: true })`.
2. If synchronization fails, retain the existing selection and usable context;
   do not proceed to linking.
3. Call `accounts.linkJWT(externalCredential)` outside any Jazz context.
4. Open a new context with the returned handle. If linking fails, a new context
   may be opened with the manager's unchanged previous handle.

Ordinary shutdown without `waitForSync` retains its local-durability behavior,
including offline teardown. The explicit graceful-sync option waits for pending
writes to reach the core and then closes. It is a general context facility, not
an account-protocol primitive. A shared client must release other holders before
its final holder requests graceful-sync teardown.

Transactions retain their complete original author. An external identity must
never upload pending local-first transactions as if it were that author. This
release uses sync-before-switch; it does not add cross-author replay authority.

## Native account roots and advisory admission

The RN platform supplies its absolute application storage directory; JavaScript
supplies no filename or path. The native account filename is lowercase BLAKE3
hex plus `.sqlite`, over these exact bytes in order:

- ASCII `jazz-native-account-root-v1` followed by one NUL byte;
- each of canonical registry URL, canonical application UUID, and environment:
  big-endian unsigned 32-bit UTF-8 byte count followed by those UTF-8 bytes;
- the account UUID's 16 raw bytes in standard UUID order.

Application names resolve through the shared application-ID derivation before
this encoding. Registry URLs use the same canonical HTTP(S) account endpoint
as the shared handle; optional ws/wss transport URLs normalize to HTTP(S) only
for comparison. The exact acting principal and active transport are excluded
from the filename. Linked principals share the durable account root only after
the prior live identity closes. Same-principal contexts share the relay while
owning independent foreground node leases and shutdown lifetimes.

The shared TypeScript JWT decoder supplies local advisory provider claims to
native admission and same-identity refresh. Rust uses the common JavaScript
numeric/value projection and reconstructs reserved author bindings from the
admitted identity. These claims cannot grant remote admission: the server
verifies the original bearer and constructs its own policy binding. Refresh
changes neither account nor acting principal. Local claims and subscriber
bindings update together; upstream renewal reconnects with the new bearer.

## Durable policy-binding directory

The directory retains the existing typed provider-claim node encoding. It
stores the exact author identity plus a native record with `derived_v1: U8` presence mask and
`claims_v1: Array<Record>` typed provider-claim nodes, omitting the canonical
identity-derived entries (`user` and its structured paths, `authMode`, and the
namespaced `iss`/`sub`). Writing omits an entry only when it equals the value derived from the
stored identity; other portable scalar bindings remain stored verbatim. The mask bits 0–7 correspond to `\0claims:iss`,
`\0claims:sub`, `authMode`, `user`, `user.account`, `user.identity`,
`user.identity.issuer`, and `user.identity.subject`. Reading rejects entries stored both explicitly and in the mask and reconstructs only the mask-selected entries from that
identity before producing the policy-binding key. Partial internal bindings
therefore remain exact; missing claims are never implicitly added. Provider
claims named `user` remain separately namespaced and are retained.

In-memory policy equality and its digest still include the complete canonical
binding. Structured author records contribute their native descriptor and
payload to that comparison encoding; process-local intern IDs never enter it.
This introduces no generic record-valued durable key or migration-lens default.
