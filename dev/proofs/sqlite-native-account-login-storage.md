# Native account login stores

Linked logins retain separate pending journals and private row projections.
Alice can close with local writes pending, Bob can open the same account and
write locally, and Alice can later resume her exact uploads under her original
identity. Bob does not see or upload Alice's pending work. Each login obtains
accepted rows through the normal authenticated sync and permission paths.

The native account host keeps the existing account-root-v1 namespace and lock.
The first login continues to use that account file. When its marker belongs to
another linked login, the host selects a deterministic sibling file for the
requested login. It never copies rows, moves journals, changes old upload bytes
or scans the directory. This matches the Node SDK's existing separation of
durable stores by account and exact login.

## Named filename encoding

`jazz.native.account-login-root.v1` derives the sibling filename from:

1. The literal bytes `jazz-native-account-login-root-v1\0`.
2. A BE-u32 length and the UTF-8 bytes of the existing account-root-v1 digest,
   exactly 64 lower-case hexadecimal characters without its `.sqlite` suffix.
3. A BE-u32 length and the UTF-8 bytes of the canonical account-bound author.

The filename is `<account-digest>.login-<BLAKE3-preimage-digest>.sqlite`, using
lower-case hexadecimal digests. Absolute OS paths do not enter the preimage.
Registry, app, environment and account remain bound by the unchanged account
digest; issuer and subject are bound by the canonical author. Transport state
and token refresh do not change either name.

`crates/jazz-native-relay/fixtures/sqlite_native_account_login_root_v1.json`
independently assembles and pins both preimages and filenames, including the
existing base account filename. Public ABI tests create both first-login orders
and inspect the actual resulting paths and v1 marker bytes. The SQLite file,
owner marker, upload journal, wire protocol and account-selection encodings do
not change.

## Admission and locking

The immutable native account-session capability retains the host-derived base
root. Generic private-scope JSON admission cannot set this account-root field.
Schema attachment makes a read-only selection and restores the selected file's
durable node. An existing login-specific file takes precedence and must have
the exact subject and scope. A malformed, foreign or substituted file is an
error; the host does not silently fall back to an empty store.

Every account login holds the old base root's `.owner.lock` inode. A sibling
store also holds its own `.owner.lock` inode. Thus a live original account owner,
another linked login, and a generic opener of the exact sibling file all remain
mutually exclusive. The host takes the account lock first, then the selected
file lock, and rechecks its selection before core opening. A changed selection
refuses that attempt. Locks remain held through runtime retirement, and their
inodes are never replaced.

Reopening Alice's old base file does not require migration. Returning Bob always
selects his existing sibling file, including after Alice has synced her queue.
Each store keeps its own durable node and sequence. Native private markers keep
their exact subject: the account-session route no longer switches that marker
to another login. Core recovery of an earlier interrupted drained handoff remains
limited to the admitted account host and requires empty original journals.

## Compatibility and scope

An earlier published SQLite reader can still open the original account file as
its exact login. It cannot discover the new sibling store and refuses the base
file's different subject. The coordinated unreleased port must be used to resume
other linked logins. No downgrade, deletion or consolidation of sibling stores
is performed.

The public ABI tests cover exact pending-upload replay for both logins, private
offline reads, real authority settlement, lock exclusion, marker preservation,
interrupted core-marker recovery and planted-store rejection. The SDK regression
uses real registry linking and the production native foreground API to check
that later accepted rows retain each login's original author.

## Retained login before the provider responds

The React Native account manager retains the selected external account's
assignment in the application's protected account store. It retains neither the
provider token nor its claims. On restart, the trusted account-session admission
route can reopen that exact login without a token. Provider claims must be empty;
reserved identities, including local-first identities without their signed
proof, cannot use this route. A malformed non-empty token is still an error.

The foreground can read local data and stage writes under the original author.
The native host keeps the configured endpoint but creates no socket worker
before a credential arrives. Calling reconnect cannot bypass that requirement.
A refresh must project to the same issuer and subject; remote verification and
account assignment remain the authority's responsibility.

The first matching credential updates claims and starts the ordinary upstream
on the existing foreground. An explicit disconnect remains in effect until
reconnect. A retained sibling borrows an already active same-identity snapshot
without clearing its token or claims. Conversely, an authenticated sibling
promotes all retained capabilities for that same scope. Credentials remain
ephemeral native state and do not alter stored bytes or filename derivation.

The SDK keeps its agreed shutdown policy: authenticated switches drain;
provisional switches preserve local work for the original identity's later
return. Native ABI tests check credential-free transport silence, identity
rejection, pending refresh and both sibling attachment orders. Public SDK tests
use a real registry and authority to check reopening pending writes, activation
without client replacement and linked-login switching with original authorship.
