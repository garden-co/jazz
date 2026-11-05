# Private account owner handoff

A trusted account host can reopen a closed private SQLite owner under a different
login of the same account once every previous upload has settled. The direct Node
account host retains its exclusive root lock through core admission and
host marker completion. No runtime is exposed between those steps. A generic
open or private scope supplied without account-host admission cannot perform the
transition, even if its author claims the same account UUID.

Native account sessions now preserve separate login stores under one account
lock, as described in the [native login storage proof](sqlite-native-account-login-storage.md).
They retain the original private marker's subject instead of using this drained
transition to switch the native file to another login.

The core checks pending standalone uploads, forwarded uploads, shadow rows,
owner input footprints and initialization publications using existing keys and
partial indexes. Any pending input refuses the switch without changing its
identity, bytes or fate. Accepted rows and provenance are retained; reads and
future uploads use the newly admitted identity and ordinary permission checks.

## Named transitions and existing bytes

Two named v1 transitions add no storage columns or wire messages:

- `jazz.sqlite.private-owner-handoff.v1` changes the canonical subject TEXT in
  the existing `jazz.sqlite.owner-scope.v1` singleton.
- `jazz.sqlite.node-account-handoff.v1` replaces the Node account-owner v1
  marker's canonical author. Its stable account owner JSON is unchanged. The
  existing header and two BE-u32-length-delimited UTF-8 fields are unchanged.

`crates/jazz/fixtures/sqlite_private_owner_handoff_v1.json` independently pins
both logins' canonical author bytes, full Node marker bytes and native scope
bytes. Native public ABI tests compare the separate login stores' marker fields
to that fixture and retain each store's own durable node. Node public binding
tests compare the complete marker files. SQLite's physical pages remain its own
format, not an application encoding.

## Admission and interrupted updates

The core subject changes first, in a FULL synchronous SQLite transaction for
the Node host. The host marker follows only after core opening, cached catalogue
validation and private owner admission succeed. Node writes a new regular file,
fsyncs it, atomically replaces the old marker and syncs the parent directory on
Unix; Windows uses write-through replacement. First-time Node marker admission
retains its non-replacing installation behaviour.

A crash may leave the old host marker with the new core subject. Both markers
must still name linked authenticated subjects in the same admitted account
namespace. The account route can resume either login: another core subject
change still requires empty pending journals. Ordinary opens retain their
exact-subject restrictions. Malformed markers, unknown versions, suffix bytes,
changed account scopes and concurrently held roots are refused.

The account host remains the trusted local admission boundary. External provider
claims do not grant remote acceptance: the authority continues to verify the
actual login and account before accepting uploads. The handoff adds no delegated
claims or permission bypass.

## Compatibility and scope

Existing v1 readers can reopen the completed state as the new exact subject.
Earlier builds cannot initiate this transition or recover its intermediate
state as the other subject. No migration or downgrade rewrite is introduced;
this belongs to the coordinated, unreleased SQLite/E2EE port.

The direct single-file API implements the drained account-host transition. It does not enable a
provisional login to take over the same root while another login has pending
work. Such attempts remain explicitly unsupported and preserve the original
owner's data. The agreed SDK behaviour remains: authenticated switches drain,
failed graceful/shared shutdown keeps the old client usable, and provisional
switches must eventually retain pending writes under their original identity.
