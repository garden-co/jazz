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
