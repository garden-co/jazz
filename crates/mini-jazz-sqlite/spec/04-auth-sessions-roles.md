# Auth, Sessions, And Roles

## 7. Auth, Users, Sessions, And Roles

Every query, write, and incoming sync application is evaluated under a session.
The session feeds policy evaluation, observed facts, sync delivery, validation,
write provenance, and error reporting.

Policy evaluation should see the same session context whether work is evaluated
in a local client, browser worker, edge server, or global authority.

An untrusted client always has an associated user for each live connection
to a trusted peer. The trusted peer authenticates that connection and evaluates
client-originated queries, sync requests, and writes under the authenticated
session. A client may reconnect with refreshed auth, but the user for a
live session must remain stable.

A trusted peer, such as an edge worker or global core, has a node identity and
authority role but no ambient user. It offers operations that execute
either:

- as a specific authenticated user, for ordinary user/service queries,
  writes, and forwarded sync validation
- as admin/system, for operational work that bypasses row policy and is
  attributed to a reserved system actor
- as a privileged backend operation attributed to a specific user, for work
  that intentionally bypasses row policy but should preserve user-facing
  provenance

Trusted-peer-to-trusted-peer sync must preserve the initiating session context
where policy validation is still pending. It must not rely on the receiving
peer's own node identity as a stand-in user.

Policy authority and write attribution are distinct concepts:

- **As user** evaluates reads and writes under that user's policies and records
  normal provenance for that user.
- **Admin/system** bypasses row policy and records provenance under a reserved
  system actor, not under an ordinary user id such as `admin`.
- **Attributing to user** bypasses row policy as a privileged backend operation
  while recording `$createdBy`/`$updatedBy` as the named user.

Current Jazz already has this product distinction: backend request/session APIs
evaluate policy with a request session, backend/admin credentials provide
privileged transport or catalogue authority, and attribution helpers stamp
write provenance without switching policy evaluation to that user. The new core
should model this directly instead of deriving both policy input and provenance
from one string.

Runtime APIs and in-memory data structures should preserve this distinction.
Opening an ordinary client runtime requires a user. Opening a trusted peer
requires only node/storage/schema identity and starts in an admin session. In
the prototype runtime API, "user" is also the protocol/security term for
recorded authorization identity. Trusted peers may then execute scoped work as
an authenticated user, but that user is session state, not part of the peer
identity. The term "principal" should only appear when describing external auth
standards, such as mapping a JWT principal/subject claim onto a Jazz user id.
This shape should be visible in tests and harnesses: topology constructors model
clients separately from trusted peers, and helpers that execute as a user must
only be valid for trusted peers.

Hosted auth integrations authenticate sessions and produce users according
to app configuration. For JWT-based auth, the app configuration chooses which
claim becomes the Jazz user id.

Local anonymous users may have durable local users, but account-linking or
migration from anonymous users to hosted users is not specified here.

Local-first auth is a product mode. A device may mint a durable local user
from a platform-generated secret without requiring login. For compatibility with
current Jazz, the baseline local-first identity is a 32-byte CSPRNG secret used
to derive an Ed25519 signing key, a self-signed Jazz JWT, and a stable user
id derived from the public key, for example UUIDv5 over the public-key bytes in
a Jazz namespace. Exact token fields may evolve, but the durable invariant is
that the same local secret reproduces the same user and clearing local
storage can lose account continuity.

Auth mode is policy input. Policies may distinguish hosted/external,
local-first, anonymous, backend, service, and admin sessions. A live client must
not hot-swap users: token refresh may update auth state only when the
user remains the same. Hybrid account upgrade should preserve identity by
binding hosted auth to the existing local-first Jazz user where possible.

Admin/system and privileged attribution sessions bypass row policy entirely.
They are still represented as sessions for audit, provenance, catalogue checks,
and operational controls. The reserved system actor namespace must not collide
with app user ids; the Rust spike currently uses `@system/admin` for the admin
session's provenance.

Untrusted clients cannot forge authority-only facts such as global acceptance,
rejection, durability receipts, or catalogue publication.

Trusted peers may accept mergeable transactions on behalf of an authenticated
session according to their policy authority role. Once an edge accepts such a
transaction, downstream clients may treat that acceptance as authoritative for
visibility in the edge trust topology; the original session authentication does
not have to be replayed by every downstream client.

When a trusted peer receives sync from an untrusted connection, policy
validation uses the authenticated user attached to that connection, not
`$createdBy`, `$updatedBy`, or any other provenance field carried in the bundle.
Bundle provenance is public data and cannot authorize a write. If the receiving
trusted peer does not know the authenticated user for a pending mergeable
transaction, it must reject or await auth context rather than infer authority
from history rows.

Exclusive transactions require final fate from the global authority. Edges and
other intermediaries forward them upstream immediately instead of waiting for
local policy dependencies. Until global fate returns, intermediaries may store
the pending exclusive transaction and its history for retry, but ordinary reads
must not show it as accepted state. The forwarded transaction must carry enough
authenticated session context for the global authority to evaluate it under the
same user, admin/trust role, and policy context as the initiating session.

The Rust prototype currently carries only an optional forwarded authenticated
user for this authority validation path. Forwarding rewrites only the selected
transaction record to pending exclusive, clears global/receipt state for that
forwarded transaction, and must not mark unrelated transactions exclusive.
Forwarded trust role, auth mode, and richer policy-context proofs remain future
work.

Non-admin sessions fail closed when required policy metadata is missing.

Application-visible provenance fields include at least:

- `$createdBy`
- `$updatedBy`

Open issues:

- exact session wire shape
- valid JWT/auth claim configuration
- exact local-first JWT validation and TTL/skew rules
- anonymous-to-hosted and local-first-to-hosted migration UX
- exact reserved namespace for system actors and whether service accounts are
  ordinary app users or system actors
- which provenance fields are visible by default under policy
