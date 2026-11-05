# SQLite upstream snapshot and local receipt port

This port retains the unreleased `JSQL/2` semantic envelope and existing
message tags. Ordinary `Fetch` (tag 3) gains a final `ReadViewSourceSpec` field.
The Current view is tag 0; BranchView is tag 1 followed by its head selector and
optional current or frozen base; Snapshot is tag 2 followed by `SnapshotRef`.
Existing selector and snapshot field encodings are unchanged. Qualified scoped
and accepted fetch messages retain their Current-only layouts and evidence.

Ordinary `Stored { tx }` is appended at tag 46. It acknowledges that an ordinary
worker holds its foreground's original transaction; it does not report an
authority fate. A private foreground ignores this unqualified message and still
requires its original-node-qualified `OwnerStoredV1` after the backing-store
flush. A new public regression checks the pending, Local and Global boundaries.

A reopened ordinary foreground asks its owner to resolve snapshot dots before
materialising the view. The reply carries the exact named commit origins and
required historical rows. No-server ordinary workers retain upstream's local
snapshot horizon. Private owners and their foregrounds continue to bound the
horizon by accepted authority state, and historical pending private inputs
remain explicitly unsupported.

`sqlite-sync-v2/fetch-view.json` independently pins all three view arms, all
branch base choices, a snapshot with a named transaction at the 127/128 varint
boundary, and Stored. The shared v2 corpus has 47 variants and 26 additional
cases. Existing historical v1 files remain byte-for-byte unchanged and rejected;
existing authority evidence, claims and receipt bytes are unchanged. These
unreleased semantic layouts require a coordinated deployment, as already chosen
for the E2EE stack. No mixed intermediate-v2 operation is promised.
