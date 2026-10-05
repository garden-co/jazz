---
"jazz-tools": patch
---

Declare encrypted table columns through the public schema builder and compose
managed lifecycle schemas and policies automatically. Previously, encrypted
scope preflight could mistake pending-deleted rows for absent rows, branch
restores could inspect the wrong branch, and mutable preview bytes could change
the values later encrypted. Plaintext-only transaction reads could also trigger
key-store enrolment.
Ordinary synchronous mutation handles continue to own encryption preparation,
new-scope recipients, and authoritative acceptance.

Now scope checks include pending-deleted rows and use the requested branch
coordinates; a missing upsert inserts rather than overwriting an unobserved
row. Preview values are isolated from caller buffers. Applications with encrypted
tables prepare their device during database startup, rather than delaying the
snapshot of an exclusive transaction. Plaintext-only applications do not enrol
a device, and plaintext transaction reads do not repeat key-store setup.

An established device can reopen offline using retained keys and accepted local
history. Call `disconnect()` after opening to use local encrypted reads without
waiting for the server. Local readiness does not prove current server approval;
first-time enrolment still needs a connection.

Ordinary reads decrypt selected logical values. Unsupported encrypted query,
subscription and migration paths fail closed until their owning layers.

Native transaction waits use transport progress edges rather than queued-frame
occupancy. Caller tasks remain runnable while an earlier asynchronous frame route
is blocked, without dropping or reordering later frames. Settlement callbacks
do not accumulate while waiting for server acceptance; transaction fates and
terminal errors are unchanged.

Tombstone-inclusive reads retain deletion state when binding session claims.
After a read grant is restored, the same client can receive fresh readable
content without discarding its local cache; revoked successor content remains
withheld.

Exact-head tombstone-inclusive reads now retain deletions whose content exists
only in an undeclared base. These rows have absent non-branch cells; naming a
current base supplies its retained body. Compatibility-omitted head content
still masks the base and never becomes a sparse tombstone. Trusted reads
continue to require selected content and read-policy authorization.

Group recovery correctness checks use the 120-second multi-client test budget,
including interrupted recovery and retry; fault injection and permission
assertions are unchanged.

Authenticated session and admitted relay writes now retain their exact verified
claim scope through final authority ingest, including exclusive predicate checks.
This prevents an unchanged protected row from producing a false
`ExclusiveConflict` after a detached preflight drops its claim scope. Parent and
schema prerequisite replay retains the original binding within the process;
changed resend bindings conflict and current policy revocation remains effective.
No implicit grant, startup validation, or wire/storage format change is added.

Rust callers must remove `CommitUnitIngestContext.admitted_write_authorization`
and replace the removed `PeerState::prove_terminal_commit_authorization` and
`NodeState::commit_unit_satisfies_write_policy` preflights with actual authenticated
ingest. There are no compatibility aliases.

Offline readiness refusal checks now initialize real foregrounds and use
confirmed `disconnect()` state. An unreachable configured server is not an
explicit disconnect: online enrolment can remain pending until connection or
cancellation. Retained-key persistent reopen with a stopped server continues to
use accepted local history.
