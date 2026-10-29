# SQLite storage format 1 corpus

What an alpha.60 user has on disk: SQLite files written by Jazz storage
format 1 (`jazz::format::STORAGE_FORMAT`), gzip-compressed and base64-encoded.
Once alpha.60 ships they are immutable: a change that a format-1 file would
not read bumps `STORAGE_FORMAT` and adds a new corpus beside this one, and
never rewrites these. Until then format 1 is still moving, so such a change
re-runs the producer and re-pins the hashes here and in the test instead
(for example: per-write timestamps on the wire changed how pending uploads
are stored, a rejection's reason name changed how fates are recorded, and
pending uploads moved from one encoded message to a row per op).

- `server.sqlite.gz.base64`: an authority (WAL, default page size). SHA-256 of
  the SQLite file `1c8043a5539e33bf83bfc55471828b05c08c3faa0e8f26e337bf8855f60a6783`.
- `client.sqlite.gz.base64`: a client replica that synced with it. SHA-256
  `f8e09e8044c2f4b41ac1743611e96f4ba149bb3a27929d97440b9c4b5ebf34a7`.

Producer: `produce_sqlite_storage_corpus_v1` in `crates/jazz/tests/storage_corpus.rs`
(ignored; `JAZZ_WRITE_SQLITE_STORAGE_CORPUS=1`). It wrote, with fixed row ids,
nodes and write times:

- `projects`, `documents` (reference, indexed title, a 300 KiB chunked large
  value, bytes, a defaulted column) and `drafts` (branched by `branch_id`);
- two interned authors, updates (history versions), a soft delete and a row
  on a branch;
- a client text edit to `documents.body` (text-merging) that the server
  merged with its own concurrent edit, so its history keeps the client's
  write as sent (`_st` bit 2, at `_seq` = minus the commit's seq);
- a client that synced `projects` and `documents`, wrote one row that
  settled, one stamped far ahead of the server's clock that the server
  rejected (so `_jazz_transactions` keeps a rejected fate with its reason name),
  then two writes (an insert and an update) and a serializable transaction
  that read one project and inserted two, none of which reached the
  server: its pending uploads, the last one stored as two op rows and a
  read row;
- a row of `drafts` on a base branch that the draft branch overrides with a
  version the client may not read: the client's view of the draft over that
  base hides the row, so the client keeps a marker of it in `_jazz_withheld`;
- then the server activated a second schema version that renames
  `documents.title` to `heading` (a stored-names lens) and wrote under it.

The receipts that read it back run by exact name in `dev/gates/storage-compat.sh`.

The greenfield removal of `_jazz_shadow` regenerates this format-1 corpus
in place, without a migration or format bump. Pending writes recover their
upstream baseline from the current row or the newest history version below
the replica's local sequence range. Eviction removes current and historical
versions so reopening cannot restore an evicted baseline.

Dropping the large-value key columns (`$k:<column>`) and `_jazz_lv_scratch`,
when queries stopped reading chunked values' content, regenerated it the same
way.

Replacing the authority's `_jazz_text_in` side table with as-sent history
versions, when `documents.body` became text-merging, regenerated it the same
way.

Keeping the deploy catalogue as typed rows (`_jazz_versions` numbering the
admitted versions, `_jazz_names` keyed by that number, `_jazz_stored_columns`,
and `_jazz_schemas`, `_jazz_lenses` and `_jazz_permissions`) regenerated it
the same way. A development store with the earlier layout (`_jazz_names`
keyed by version id) is refused at open with "delete the local store and
resync".

Moving a commit's bases, access proofs (formerly sources) and deltas onto its
ops, so each pending op is stored with them and `_jazz_pending_items` keeps
only ops and serializable reads, regenerated it the same way.

Replacing a pending commit's `author` and `session` fields with one `acting`
enum (`Own`, `Attributed`, `Session`), which changes the encoded
`_jazz_pending` header, regenerated it the same way. A pending row in the
earlier layout is refused as an unsupported storage format, never misread.

Merging `_jazz_applied` and `_jazz_settled` into one `_jazz_transactions`
table keyed by an interned node code (`_jazz_nodes.n`) regenerated it the
same way.

Deleting a settled commit's pending rows in one statement (the
`_jazz_pending_forget` trigger on `_jazz_pending`, with
`_jazz_pending_versions` now created on open) regenerated it the same way,
adding the serializable pending commit above so the per-op layout is
pinned with more than one op and a read.

Hiding branch-view rows whose head version the reader may not read added the
client's `_jazz_withheld` table (created on the first hidden eviction) and
gave `drafts` an `owner` column and a read policy, which regenerated it the
same way, and re-pinned the browser corpus (whose producer hides a note the
same way). A storage change re-pins this corpus and the browser corpus;
`server-data-dir-v1` is re-pinned only when an authority's bytes change (an
authority never writes `_jazz_withheld`, and that producer is not
byte-deterministic).
