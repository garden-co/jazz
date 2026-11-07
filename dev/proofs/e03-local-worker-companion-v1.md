# E03 local worker companion v1

The foreground and its durable worker exchange normal sync frames. A separate,
optional local lane carries the original per-operation provenance captured by
core C before stored-name lowering. Missing or refused provenance leaves an
ordinary valid Commit on its normal admission, permission and receipt path.

## Trust boundary

The application embedding, its generated native/WASM binding, and its private
worker bootstrap code are trusted. They own the actual foreground runtime and
the worker's admitted transport. The MessagePort is a capability of that local
embedding; it must not be forwarded to a peer or exposed as a relay endpoint.
Application code that compromises that embedding is outside this boundary.

`enableLocalSubmissionSource` is called only by the local follower carrier,
after worker admission. Core builds the source descriptor from the actual Db
namespace, node, author and a fresh incarnation, and exports only existing
pending records. No host argument selects original schema, claims or bundle.
The worker's `attachLocalSubmissionCompanion` is a privileged host entrypoint
on an already-admitted subscriber link. It creates a port bound to that exact
receiver owner, epoch, slot and generation. Ordinary `sendWireFrame(s)` never
decodes companions or creates a port. There is no generic Deserialize constructor
for a C export, and no new upstream Commit, SyncMessage, signature or auth
protocol. The local descriptor is not a credential or proof of locality by itself.

The embedding attests that this private lane connects its actual local owners.
It must never pass a descriptor or companion obtained from network traffic into
this API. Bytes on this lane are not independently authenticated against a
malicious embedding. The frame fingerprint checks exact Commit equality; it is
not a MAC. No session trust, claims, upstream pin or authority SELECT verdict is
created by local attachment. Unknown upstream policy context stays unknown.

## Local encoding

All integer fields below are little endian. This encoding is local control
data, not a storage format or a change to normal sync frames.

Descriptor: ASCII `JZLCAP01` (8 bytes), authority namespace (16), producer node
(16), producer author digest (32), fresh source incarnation UUID (16): 88 bytes.
The author digest uses the existing SHA256 derive_key encoding with context
`jazz.local-submission-author.v1` and the author's canonical UTF-8 representation.

Companion: descriptor, monotonically increasing source serial u64, exact pending
transaction time u64, existing `jazz.local-submission-frame.v1` fingerprint (32),
operation count u32, then one operation entry in original order. Entry tag 0 has
no payload; tag 1 has original schema UUID (16) and original scope digest (32).
No other tags or trailing bytes are accepted. Transaction node comes from the
descriptor and must match the producer's actual pending Commit. Full u64
transaction time is preserved. Unrecognised versions are refused.

Companions are at most 4 MiB and at most 64 may wait on one admitted session.
Overflow revokes the optional port and clears its unconsumed metadata. Duplicate
or reversed serials are refused. When a Commit reaches normal queue processing,
its transaction id, exact fingerprint and operation count must match; otherwise
it gets no imported observation provenance. Companions do not hold row bodies.
Normal frame splitting and reassembly remain in the existing bounded FrameJoiner.

## Lifetime and persistence

The binding captures companions after the core tick, outside its Store borrow,
while draining actual upstream frames. The private carrier sends companions
before the corresponding frame batch. The receiver attaches them to its actual
admitted session before its normal pump receives those frames. Session generation
and owner epoch checks apply again at Commit execution, not just attachment.
Closing a link revokes its port; closing the owner retires its C capabilities.
Worker replacement, storage invalidation or channel disposal removes the relay
and rejects pending observation requests. Already-dispatched external I/O cannot
be undone by later revocation.

Content and imported pending metadata are written in the same existing SQLite
savepoint; a persistence/rebase failure rolls both back. Browser Local/Stored
acknowledgement additionally requires the failure-aware physical-file flush
owned by OPEN #4308. Direct storage-owner retirement requires OPEN #4309 and C's
capability invalidation before that retirement. The original C50d6 base has
neither complete host prerequisite; tests on that base cannot qualify their
durability or replacement behaviour. No competing flush/retirement implementation
is introduced here.

## Observation consumer

`Db.observeE2eeHistory` accepts nonempty `__e2ee_` queries on one executing schema
handle. It hydrates using ordinary local-first reads and discards those results.
It then asks the durable owner for one synchronous `observeLocalBatch`. The
request must match the durable owner's actual schema view; it cannot install a
caller-declared schema or governing policy. An unavailable view is refused.

Observation executes on the actual admitted Link, using existing session_db
identity, fixed-claim choice and authenticated governance. Foreground reserved
session tokens are resolved only in their original realm and never forwarded
as worker credentials. The request's claim values are equality operands checked
against the existing core admission after canonical normalization, not a way to
select identity or policy. Observation publication uses the existing admission/auth
transition queue so a rebind cannot overtake it. Raw worker provider claims are
not compared to foreground claims augmented with identity operands.
Closed/replaced slot generations and synchronously revoked links refuse reads.

The core batch prepares each query through existing machinery, captures one C
cut, and pairs every returned row with exact accepted state/content origins.
It excludes pending prefixes while ordinary transactions retain their existing
semantics. The batch opens no ordinary transaction and retains no cut across
host turns; success and errors drop the cut without leaving rollback state.
The adapter checks result count, shared through coordinate, unique row/evidence
pairing, origin bounds and provenance tags. These are partial accepted local
candidates, not completeness or authority SELECT proof.

The persistent-browser regression retains the original root/reference Global,
shutdown, server-unavailable reopen and ordinary local-first assertions. Mocked
worker tests are reported separately from real SQLite, WASM and browser runs.

Canonical session operands: standard policy binding supplies structured `user`
and `user.identity` records. Scope codec v1 reserves tag 17 only when a top-level
record exactly equals the core-derived binding for the already-encoded author
and key. The identity and key determine its complete value; no record, issuer or
subject supplied by a caller selects authority. Other records remain unsupported.
Previously supported scalar preimages are unchanged; formerly unsupported standard
canonical records now have an independently tested deterministic representation.

The admitted observation cut keeps provenance identity separate from the actual
core `session_policy` mode. An own-identity replica uses the same permissive-local
mode as normal serving; authority/foreign sessions keep their applicable SELECT
checks. Only core derives this mode from the live slot/generation, never a host
flag. SYSTEM follows the actual owner's local operands. Admitted cuts check that
session generation, effective claims, names and governing context remain live at
use; public C cut semantics are unchanged. None of these local candidates is an
authority SELECT verdict or an inferred upstream pin.
