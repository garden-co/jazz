# E2EE schema migrations: design note

Status: decided design (§8); nothing here is implemented. It covers schema
changes that touch encrypted data when the app files (`jazz.app.json`,
`jazz.permissions.json`) are the source. References: the
[protocol](E2EE_PROTOCOL.md) ("spec §n"), the
[core hooks note](E2EE_CORE_HOOKS.md) ("H1"–"H11"), the
[crypto format](E2EE_CRYPTO_FORMAT.md) and
[lenses and migrations](10_lenses_migrations.md) ("ch. 10").

## 1. Constraints this design rests on

- **The server cannot transform ciphertext.** Any change to stored ciphertext
  or equality tokens is a write by a client that holds the space key.
- **What a cell is bound to.** The cell-record context (spec §4.1) binds the
  shared domain `json([appId, env])`, the scope table's catalogue identity,
  the space identifier, the table and column catalogue identities, the row,
  the epoch, and `T`, the canonical JSON of the logical plaintext type
  (including nullability, enum variants and JSON schema). Equality tokens bind
  the same identities without the row. Changing any of these makes old cells
  `invalid-ciphertext`.
- **Catalogue identities follow stored names** (ch. 10 §10.7). A rename lens
  keeps the stored table or column, and so its identity. A column whose stored
  value type changes, or which a lens adds, is a new stored column with a new
  identity.
- **The `encryption` entry is outside the schema hash** (H1). Today, editing
  `encryption.columns.body` from `"string"` to `"int"` changes no stored
  column, so the deploy accepts it and every existing cell then fails to
  authenticate. Section 3.1 closes this.
- **History is never rewritten** (ch. 10 §10.1). Encrypting a column does not
  remove earlier plaintext from row history, from replicas that synced it, or
  from backups. Read-access eviction is not redaction (`INV-RLS-6`).
- **Encrypted and tag columns are non-null `bytes`** (spec §1.3). A null
  plaintext is encrypted like any other value. Two short values are never
  ciphertext (spec §4.1): the empty byte string ("never written", read as the
  declared default; when stored rather than lens-supplied it is also reported
  `empty-cell`) and the 5-byte cleared marker `4a 45 32 58 01` ("moved by
  a conversion").
- **Writes cannot move a row between spaces** (spec §5.1), and a write needs
  a held key for the row's space (spec §5.1); the `move` write of §4.4 is the
  one exception, stated in the protocol too. Creating a space needs Jazz
  read and update permission on the space row (spec R15, §3.2); the client
  does not evaluate it and creates optimistically, in the transaction of the
  first encrypted write that needs the space.
- **Keys go to members only** (spec §1.3, R13). A space table's read rule
  must come from the members table, so encrypting data under an existing
  table makes its readers members first (§4.1).

## 2. Which changes are lens-only

| Change                                                     | Stored ciphertext               | Mechanism                                                    |
| ---------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------ |
| New encrypted table                                        | None exists                     | Lens `createTables`; spaces created on first encrypted write |
| New encrypted column on an encrypted table                 | None exists                     | Lens `add` with the empty-bytes default (§4.5)               |
| Rename an encrypted column                                 | Unchanged                       | Lens rename of the column **and** its `__e2ee_eq_` column    |
| Rename an encrypted table, its scope table or space column | Unchanged                       | Lens rename; identities kept                                 |
| Drop equality tagging                                      | Unchanged                       | Lens drop of the tag column                                  |
| Encoding-compatible type widening                          | Unchanged                       | Pinned `sealedType` (§4.3); no rewrite                       |
| Other plaintext type change                                | Rewrite                         | Re-encrypt into a new column                                 |
| Add equality tagging                                       | Cells unchanged; tokens written | Tag backfill                                                 |
| Encrypt a plaintext column / table                         | Rewrite                         | Encrypt conversion                                           |
| Decrypt to plaintext                                       | Rewrite                         | Decrypt conversion                                           |
| Change scope table; move rows between spaces               | Rewrite                         | Re-home conversion / move write                              |

Lens-only changes need nothing beyond what the deploy already checks, plus the
transition rules in §3.1. Every other change is a **conversion**: a pair of
columns and a client-run rewrite between them.

## 3. Recommended design: conversion pairs run by key holders

### 3.1 Requirement: encryption declarations are per schema version

- Every schema version records its tables' `encryption` entries: each
  `history` version in `jazz.app.json` carries them, as the current `tables`
  do, and the writer keeps them when it moves a version into `history`.
- Loaders return the declarations of every shipped version.
- `generate`, `validate` and the deploy check every migration against the
  declarations of its two versions, using the table in §2. A change of an
  encrypted column's `T` (other than a pinned widening, §4.3), scope or space
  that is not a rename and is not expressed as a conversion is refused, with a
  message naming the conversion to write. The deploy must refuse it even when
  the stored schema is unchanged, as it is for a plaintext type change.
- The entries still add nothing to the schema hash (H1); the deploy request
  carries every version's declarations so the server can check them.

### 3.2 The conversion pair

A conversion names a **source** column (the old form) and a **target** column
(the new form), both stored on the same row:

| Conversion | Source                                | Target                                    |
| ---------- | ------------------------------------- | ----------------------------------------- |
| Encrypt    | Plaintext column, made optional       | New encrypted `bytes` column (+ tag)      |
| Decrypt    | Encrypted column                      | New optional plaintext column             |
| Re-encrypt | Encrypted column (old type)           | New encrypted column (new type)           |
| Re-home    | Encrypted columns under the old space | New encrypted columns under the new space |

The source is **absent** when it is null (plaintext) or the cleared marker
(encrypted; a real cell record is at least 47 bytes). The target is absent
when it is null (plaintext), empty or the cleared marker (encrypted). One rule
decides a row's value:

> **If the source is present, it is the value; otherwise the target is.**

Every write by a client that knows the conversion writes the target and
clears the source (to null, or to the cleared marker for an encrypted source)
in the same row write. A client on the earlier version writes only the
source. So a present source always means "written in the old form, not yet
converted", and the logical column keeps last-writer-wins semantics under
concurrent old and new writes. The rule needs no per-row state, no progress
table and no ordering metadata.

**Nullable plaintext sources are refused.** The rule needs "absent" to mean
only "cleared by conversion". For a nullable plaintext source, null is also a
legitimate value: after a conversion clears `body` to null and writes the
target `"hello"`, an old client's write of `body = null` is again just a null
source, so the row would keep reading `"hello"` and the later null would be
lost. `generate`, `validate` and the deploy therefore refuse an in-place
encrypt conversion whose plaintext source column is nullable, naming the
copy-to-new-table recipe (§5.1) instead. Encrypted sources (decrypt,
re-encrypt, re-home) are unaffected: their stored column is non-null bytes and
a null value is an ordinary cell, so the cleared marker is unambiguous.
Nullable encrypted columns otherwise work normally, as targets included.

The app file states the pair in the `encryption` entry. Encrypt, decrypt and
re-encrypt are per column; re-home is per entry:

```json
"encryption": {
  "space": "projectId", "scope": "projects",
  "columns": {
    "body":  { "type": "string", "from": "bodyPlain" },
    "notes": { "type": "string", "to": "notesText" },
    "count": { "type": "int", "from": "countV1", "convert": "countFromText" }
  },
  "equality": ["body"],
  "backfill": []
}
```

- `from` names the source of an encrypt (a plaintext column) or a re-encrypt
  (an encrypted column no longer listed in `columns`; its type and
  `sealedType` come from the previous version's declaration).
- `to` names the plaintext target of a decrypt; the column stays in
  `columns` with its type until finalised.
- `convert` names a conversion function between the source and target types,
  implemented in every language like a `computed` field. It is required when
  the types differ and refused when they are equal.
- `reverse` optionally names, for a `convert`, a function returning the
  complete and exact list (at most 16) of source values that convert to a
  given target value. Only with it can equality queries reach unconverted
  source rows by index (§3.3).
- `backfill` lists equality columns whose tokens are still being written
  (§4.3).
- A re-home adds `"rehome": { "space": <old space column>, "scope": <old scope> }`,
  and every column carries `from` naming its old encrypted column.

### 3.3 Reads while a conversion is partial

The read hook (H3) applies the rule per row and column. A present plaintext
source is returned as the value; a present encrypted source is decrypted with
its own context (the old type, or the old space for a re-home). Rows are never
dropped for being unconverted, so a half-converted table reads as one table.

Equality queries (H4) on a converting column must not send the searched value
to the server, and must not scan unconverted rows that could match (a cost
that grows with unrelated data):

A converting column's logical value is the source when present, otherwise
the target (§3.2), and an equality query must match exactly that value. The
physical query therefore has two tag predicates, one per column:
`__e2ee_eq_<source> IN sourceTokens OR __e2ee_eq_<target> IN targetTokens`,
each with its own context (source type or old space, and target), and the
residual applies **source-present precedence**: a row with a present source
matches iff its source, decrypted under its own context and converted,
equals the searched value, whatever its target or target tag holds (an
older target or tag left by an earlier write is ignored); a row whose
source is absent matches iff its target does. Residual comparison removes
false candidates but cannot add omitted ones, so the source predicate must
reach **every** source value that converts to the searched value. That is
possible only in two cases:

- **Identity conversions**: a re-home, and a re-encrypt without `convert`.
  The source value equals the target value, so `sourceTokens` are the
  searched value's tokens under the source context.
- **A declared bounded reverse mapping**: a re-encrypt whose `convert` names
  a `reverse` function (`"reverse": "<name>"`, implemented in every language
  like `convert`) that returns, for a target value, the complete, exact list
  of source values that convert to it, at most 16 values. `sourceTokens`
  covers each of them. A `reverse` that cannot list the exact preimage (a
  string-to-integer conversion maps `"1"`, `"01"`, `"+1"`, … to 1) must not
  be declared.

The source column must also be equality-tagged. Otherwise (an arbitrary or
non-injective `convert` without `reverse`, or an untagged source) the source
rows cannot be found by index, and an equality query on the target column
fails with `conversion-pending` while any row of the space has a present
source, as below. Required tests: a non-injective conversion with an
unconverted source row that converts to the searched value gives
`conversion-pending`, not a result without that row; and a row whose
present source does not match but whose stale target tag does is not
returned.

- **Encrypt and tag backfill** have rows with no token. An equality query on a
  space that still has such rows fails with a typed `conversion-pending` error
  (spec §7) naming the table, column and space and the pending count, and the
  runtime moves that space to the front of its sweep (§3.5) if it can sweep
  it. The check is one indexed count (source present, or tag empty, in the
  space); the runtime runs it as an ordinary read before planning and hands
  the result to the query hook, which never reads `Db` itself (H4).

### 3.4 Writes while a conversion is partial

- The write hook (H2) writes the target and clears the source in the same
  `WriteCells`, for every insert, update and upsert that supplies the column.
  An update that leaves the column out leaves both as they are.
- A row in a space with no root yet: the first conversion or encrypted write
  stages the space creation (spec §3.2) in the same transaction as the row
  write, and the authority accepts it only from a session that may read and
  update the space row. A refusal fails that transaction alone. A refusal
  of the root itself suppresses automatic creation of that space on the
  device, so its later encrypted writes to the space are refused
  `space-pending` until a valid root exists or the suppression is lifted
  (an explicit `createSpace`, or a visible change of the account's member
  rows or the space row, spec §3.2). Within one transaction the row is never
  observable encrypted under a space that was not accepted.
- A decrypt writes plaintext. It is refused unless the deploy confirmed the
  decrypt (§5).

### 3.5 Who converts

| Work                                   | Who                                                                                                                       |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Create missing spaces                  | Sessions that may read and update the space row, as part of their sweep, and on seeing its member rows (spec §3.2)        |
| Convert a row the client writes anyway | Any writer (it holds the key by spec §5.1)                                                                                |
| Background sweep of a space            | By default, online sessions that may update the space row and hold its key; `"sweep": "members"` opts in every key holder |
| Report progress                        | The server, without keys (§3.6)                                                                                           |

The sweep is a client-runtime task above `Db`, like deliveries (H11): for
each space it sweeps (it may update the space row, or every member may) and
holds a key for, it subscribes to rows with a
present source, and converts them in serializable transactions of bounded
size (default 100 rows). Each transaction re-reads the rows it converts, so a
concurrent edit or a second sweeper makes it conflict and retry rather than
overwrite. A sweeper starts at a random row of its space, so several sweepers
rarely collide. Sweeping needs Jazz update permission on the rows; a sweeper
without it skips them and they appear in progress. A sweeper that may update
a space row without a space creates the space in its first conversion
transaction (§3.4), unless creation of that space is suppressed on its
device (spec §3.2). The client does not evaluate permissions: a sweeper
learns that it may not create or convert from refusals. After a refused
conversion it stops sweeping the space; after a refused creation it follows
the creation suppression of spec §3.2. The repair of a refused or lost
creation (spec §3.2) uses the same serializable, bounded, re-reading
pattern as this sweep.

A sweep starts only when the authority's active schema is the converting
version or later (§3.7). It pauses when the session goes offline; conversions
are never staged offline.

### 3.6 Progress and resumption

Progress is in the data: a row is pending iff its source is present. Presence
is visible without keys (a null or the cleared marker), and the space column
is plaintext, so the server can count pending rows per table and per space.

- `jazz-tools migrations status` asks the authority for those counts (an admin
  read) and prints them, with the spaces that have no root yet and the rows
  whose space column is null.
- In the client, `db.encryption.migrations.status()` counts what the session
  can see.
- Resumption needs no cursor: a sweeper that restarts re-subscribes to
  pending rows. Conversions are idempotent; converting twice writes a fresh
  ciphertext of the same value.

### 3.7 Mixed-version clients

| Client                                  | Unconverted row | Converted row                                                             | Its writes                                          |
| --------------------------------------- | --------------- | ------------------------------------------------------------------------- | --------------------------------------------------- |
| Before the conversion (plaintext body)  | Reads plaintext | Reads the lens's backward default (e.g. `""`)                             | Write the source; refused once sealed (below)       |
| Before a decrypt / re-encrypt / re-home | Reads as before | Source is the cleared marker: the row is reported `superseded`, not shown | Write the old encrypted source; refused once sealed |
| Knows the conversion                    | Reads source    | Reads target                                                              | Write target, clear source                          |

Two levers keep this short and safe:

- **Activation gate.** A client that knows the conversion reads both forms
  from the start, but writes the old form, and does not sweep, until the
  authority's active schema reaches the converting version. The app ships the
  client first and activates when enough clients have it, so old clients see
  placeholders only after activation.
- **Seal.** A permissions revision that adds `bodyPlain IS NULL` to the
  insert policy and to the update policy's `new` side. Policies see data
  projected into the pinned schema (`INV-LENS-19`), so an old client's write
  of `body` arrives as `bodyPlain` and is refused. After the seal, a new
  client's update of an unconverted row converts it in the same write (H2
  fetches the key first); old clients can still update other columns of
  converted rows, but no new old-form value enters the table. `generate`
  writes the seal as a suggested edit to `jazz.permissions.json`.

The `superseded` report code is new: a cell holding the cleared marker in a
column for which the reader's declaration names no conversion. It, and
`stale-space`, are protocol data-error codes (spec §4.1, §4.3, §7), so
readers shipped from the first release know them, and rows converted by a
later version degrade to a reported gap rather than `invalid-ciphertext`. An
empty cell is never `superseded`: it means "never written" and reads as the
declared default (§4.5).

### 3.8 Rows no online client can convert

| Situation                                            | Effect                                                                                 |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Encrypt; space has no root, no updater online        | Rows stay plaintext and readable; counted under "no root yet"                          |
| Encrypt; space exists, no key holder online          | Rows stay plaintext and readable; counted per space                                    |
| Encrypt; space column null                           | Cannot be encrypted; counted as "no space"; the app must assign a space or delete them |
| Decrypt / re-encrypt / re-home; no key holder online | Rows stay in the old form, readable to key holders only                                |
| Any; the space's keys are lost to everyone           | Rows are unrecoverable, as before the migration                                        |

Nothing breaks while a conversion stays open, except equality queries on the
affected spaces (`conversion-pending`, §3.3); the source column remains. Finalising (dropping the
source) is therefore refused while any row is pending, unless the developer
passes `--abandon`, which lists the spaces whose rows will then read as the
target's default (or, for encrypted sources, be reported `superseded`). The
data itself stays in history and in earlier versions either way.

### 3.9 Offline writes and merge conflicts

- An offline write by a client that knows the conversion writes the target
  and clears the source; when it syncs, per-column last-writer-wins and the
  rule of §3.2 give the same value as a plain last-writer-wins column would.
  This holds because the source is never a nullable plaintext column (§3.2).
- Sweeps and moves are serializable and online-only, so they never race
  silently with edits.
- One race remains for re-homes and moves: an update encrypted by a client
  that has not yet seen the row move seals its cell under the old space. The
  reader resolves the cell's epoch to its space through the held-key records
  (each record names its epoch and its lineage from the space root, spec
  §6.2, so each epoch belongs to one space), decrypts it under that space if a
  key is held, and reports it `stale-space`; the next key holder to sweep
  re-encrypts it. A reader that holds no key of that space reports it like
  any other missing epoch (`key-pending` for a member, `key-unavailable`
  otherwise, spec §4.3).

## 4. The five kinds of change

### 4.1 Encrypting a plaintext column or table

The developer marks the column encrypted in `jazz.app.json`. If the plaintext
column is nullable, the tool refuses and points to the copy recipe (§3.2,
§5.1). Otherwise `jazz-tools migrations create` writes the lens: rename `body` → `bodyPlain` and make
it optional (same stored column), add `body` as `bytes` with default `""`, and
add `__e2ee_eq_body` likewise if it is tagged. The entry gets `"from":
"bodyPlain"`. The backward direction maps a null `bodyPlain` to the column's
`backwardsDefault`, so old clients' required fields stay well-typed.

Making a plaintext table encrypted additionally chooses its space:

- `"space": "id", "scope": "<this table>"`: every row is its own space; the
  sweep creates one space per row, and only sessions that may update that
  row can.
- `"space": "<reference column>", "scope": "<referenced table>"`: rows share
  their parent's space. The column must be a reference to the scope table and
  must be immutable from now on (spec §5.1); `generate` warns when existing
  policies or code update it.

**Members first.** A space table's read rule must come from the members
table once any table encrypts columns in its spaces (spec R13). An existing
app therefore moves in two deploys:

1. Declare the table's `encryption` entry with its `space` and `scope` but
   no encrypted columns yet (`"columns": {}`). This adds the E2EE system
   tables (under the schema-hash decision of the core hooks note) and makes
   the scope a space table, so sessions that may update a space row can
   write its member rows, while the read rule stays as it is (the deploy
   warns). The app then writes the member rows: for each space row, one row
   per account that should keep reading it, typically from its own
   membership data, from a client of an updater or a trusted backend.
   Declaring the space table also records its `creatorSince` time (spec
   §1.5): the creators of rows that exist now gain no right to add
   themselves, and only rows created from then on have a creator who may
   (spec R12). Encryption therefore makes nobody a permanent owner of an
   existing row. A space supports at
   most 1,000 member accounts (spec §3.8): a space row that more accounts
   read today needs a narrower readership or another space table before it
   can be encrypted. The client refuses member rows beyond the limit; a
   trusted backend that writes them without the E2EE runtime must keep to
   it itself.
2. Change the space table's read rule to `exists __e2ee_members(...)` (spec
   §1.5) and add the encrypted columns. Accounts without a member row lose
   read access to the space row at this deploy. From then on, a client that
   creates a space row adds its own first member row by the creator clause
   (spec §1.5, R12), and the space table's insert rule must not need a
   member row of the new row.

Permissions that read the column's value are refused by the deploy: once
encrypted, the server sees only bytes.

### 4.2 Decrypting back to plaintext

The inverse pair: lens adds the optional plaintext target, the entry gets
`"to"`, and sweepers decrypt and write plaintext, clearing the ciphertext and
its token. The deploy requires an explicit confirmation
(`--confirm-decrypt notes.body`), because the server and every replica then
hold the plaintext. Rows in spaces no key holder reaches stay encrypted (§3.8).

### 4.3 Renames, type changes and equality tagging

- **Rename.** Lens-only. The tool pairs `__e2ee_eq_<old>` → `__e2ee_eq_<new>`
  with every encrypted-column rename, and the transition check refuses a
  rename that leaves them unpaired.
- **Type change.** `T` is part of every cell's context, and the cell does not
  record which `T` it used. Each encrypted stored column therefore keeps one
  `T` for life: the type it was introduced with, recorded as `sealedType` in
  the entry whenever the current `type` differs. A change is allowed in place
  only when every old value's native encoding decodes to the same value under
  the new type: making the column optional, appending enum variants, and
  loosening a JSON schema, subject to core confirming that the native encoding
  is unchanged in each case. Any other change is a re-encrypt into a new
  column, with `convert` when the conversion is not the identity.
- **Adding equality tagging.** The lens adds `__e2ee_eq_<column>` with default
  `""`, and the column is listed in `backfill`. Sweepers decrypt each cell and
  write its token (and nothing else) under the cell's own epoch. Until every row of
  a space has a token, equality queries on it fail `conversion-pending`
  (§3.3). Unlike the conversions, an
  old client's update writes a new cell without a new token, leaving a stale
  token; the seal cannot detect it. Backfill therefore needs either the write
  floor in §6 or re-encryption into a new tagged column (whose seal works as
  in §3.7).
- **Removing equality tagging.** Lens drop of the tag column. Tokens remain in
  history; they disclose equality only.

### 4.4 Changing the space: re-homes, moves, splits and merges

- **Renaming the space column or the scope table.** Lens-only: the context
  binds the scope table's identity and the space identifier value, not the
  space column, and a rename keeps both.
- **Pointing the table at another scope table** is a re-home: the lens adds
  a new space column and new encrypted columns, and each sweep transaction
  converts whole rows (all encrypted columns and the new space value at once).
  The sweeper needs keys of both the old and new space and the right to write
  the row; target spaces that do not exist are created by sessions that may
  read and update their space rows (spec §3.2).
- **Moving one row between spaces of the same scope** is a new write kind,
  `move`: it re-encrypts every encrypted column of the row under the target
  space and changes the space column in one serializable transaction. It
  needs a held key for both spaces and Jazz update permission. Partial moves
  are refused. This relaxes spec §5.1 for that write kind only.
- **Splitting** a space is creating a new scope row and moving a subset of
  rows into it; **merging** is moving all rows of one space into another and
  then deleting the empty scope row.
- A move is not revocation: members of the old space keep the old
  ciphertext and its history. To exclude someone, delete their member rows
  (spec §3.5).

### 4.5 New encrypted tables and columns

Confirmed trivial, with one exception. A new table has no rows; its first
inserts create spaces as usual. A new encrypted column on an encrypted table
is a lens `add` of a `bytes` column with default `""`; existing rows read the
declared plaintext default, or null for an optional column, until written. A
required column with no default is refused by `generate`. The exception: a new
equality-tagged column on a table that already has rows starts in `backfill`,
because those rows have no token for the default value; sweepers write the
default's cell and token.

## 5. Rollout for app developers

| Phase       | Developer action                                                                    | Tooling                                                                                                                                       |
| ----------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Declare  | Edit `jazz.app.json`: mark the column encrypted (or change type, scope, tagging)    | `migrations create` writes the lens, the pair (`from`/`to`/`rehome`/`backfill`), `sealedType`, and TODOs for `convert` and `backwardsDefault` |
| 2. Ship     | Deploy (publishes, does not activate); release clients built from the new files     | `deploy --no-activate`; `validate` checks the transition                                                                                      |
| 3. Activate | Activate when client adoption is sufficient                                         | `deploy --activate`; clients start writing the new form and sweeping                                                                          |
| 4. Seal     | Apply the suggested permission edit                                                 | `generate` proposes the seal policy; `deploy` publishes the revision                                                                          |
| 5. Watch    | Make sure updaters open the app; fix spaces with no root, no space or no key holder | `migrations status`                                                                                                                           |
| 6. Finalise | Optional: drop the source                                                           | `migrations create --finalise` refuses while rows are pending, or lists them with `--abandon`                                                 |

Phases 3 and 4 may be one deploy when old clients' read-only degradation is
acceptable. A decrypt requires `--confirm-decrypt` at phase 2.

**Earlier plaintext stays visible.** Encrypting an existing column protects
only values written from then on. Its earlier plaintext remains in row
history on the server, on replicas that synced it and in backups. If that is
not acceptable, do not convert in place: copy the data into a new encrypted
table (§5.1).

### 5.1 Recipe: copy into a new encrypted table

1. Add the new table to `jazz.app.json` with its `encryption` entry, and
   `createTables` it in the migration; deploy and ship.
2. In a client that may read and update the space row, read the old rows of
   a space and, in one serializable transaction, insert their copies into
   the new table. When the space has no root yet, the same transaction
   creates it (spec §3.2), so the first encrypted rows and the space are
   accepted together.
3. Point the app's code, relations and permissions at the new table; rows get
   new IDs, so references to them must be rewritten in the same transactions
   or mapped by the app.
4. Delete the old rows, then drop the old table in a later migration. The old
   plaintext stays in the old table's history, which no new version reads;
   the server operator removes it, with its backups, outside Jazz.

## 6. Core hooks and state

| Need                                                                                                   | Where                                                                    |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| Per-version `encryption` in `history`; transition check                                                | `app_file.rs` reader and writer, `migration_validation.rs`               |
| Declarations carry `from`, `to`, `convert`, `sealedType`, `backfill`, `rehome`, `sweep`                | H1 `E2eeDeclarations`                                                    |
| Write target and clear source; tag-only writes; `move` write kind                                      | H2 (`WriteCells` gains the source cells; a `Move` kind with both spaces) |
| Source-or-target selection; short values; `superseded` and `stale-space` reports; epoch → space lookup | H3, from held-key records (spec §4.1, §6.2)                              |
| Tokens for both contexts (identity or declared `reverse` only); the `conversion-pending` check         | H4                                                                       |
| Sweep task, activation gate, status API                                                                | Client runtime above `Db`, beside deliveries (H11)                       |
| The client can read the authority's active schema version                                              | Core catalogue API (new)                                                 |
| Lens: rename plus optionality widening on one stored column, with a backward default for null          | Core lens (to confirm)                                                   |
| Native encoding unchanged under each allowed widening                                                  | Core values (to confirm)                                                 |
| Pending-row counts per table and space                                                                 | Server admin read; CLI `migrations status`                               |
| Optional write floor: the authority refuses commits to a table authored below a version                | Core sync admission; an explicit exception to `INV-LENS-16`              |
| Conversion functions named by `convert` and `reverse`, implemented in every language                   | Bindings, like `computed`                                                |

No private local state is needed: progress lives in the rows. A sweeper may
cache its position in memory; it is never authoritative.

## 7. Alternatives considered

- **Copy into a new table** (the app reads the old table and inserts into a new encrypted one in a
  serializable transaction that also declares the space). It needs no core
  support, but row IDs change, references and permissions must be redone,
  and there is no shared progress or mixed-version story. It is the
  documented recipe (§5.1) when earlier plaintext must not stay readable, and
  the fallback for restructurings this design does not cover.
- **In-place rewrite of the same column.** Impossible for encrypt and decrypt,
  since the stored type changes between `bytes` and the plaintext type, and
  for re-homes it leaves readers unable to tell which space a cell uses.
- **Read-time dispatch instead of rewrites** (try each historical `T` or
  scope until one authenticates). It needs no sweep, but every read pays for
  every past declaration forever, and a plaintext column could never be
  encrypted this way.
- **A key-holding migration worker** (a service account that may read every
  space row, running sweeps server-side). It finishes promptly and reaches
  cold spaces, but gives one server-run identity every key, which is what
  E2EE excludes. Apps may still do this explicitly, by making such an
  account a member of the spaces.
- **Lazy conversion only** (convert on write, no sweep). Cold rows stay in the
  old form indefinitely: plaintext stays on the server and finalising never
  becomes possible.

## 8. Decided

1. In-place conversion pairs with the "present source wins" rule and client
   sweeps (§3); copying into a new table is the recipe of §5.1.
2. Online sessions that may update the space row sweep by default;
   `"sweep": "members"` opts in every member holding the key.
3. New-form writes and sweeps wait for the server to activate the converting
   schema, through a client-visible active-schema API.
4. The seal is a permissions revision that tooling proposes and the developer
   applies.
5. Encoding-compatible widenings keep a pinned `sealedType`; every other type
   change re-encrypts into a new column.
6. Equality tags are backfilled in place, with an opt-in core write floor per
   table.
7. Rows move between spaces through a serializable, online-only `move` write;
   stale-space cells are decrypted by their epoch and repaired.
8. Decryption is allowed with an explicit per-column deploy confirmation.
9. Finalising is refused while rows are pending, unless `--abandon` lists the
   affected spaces.
10. Encrypting an existing column leaves its earlier plaintext in history,
    replicas and backups; this is documented, with no latest-only remedy and
    no core purge. Where that is unacceptable, copy into a new encrypted table
    (§5.1).
11. The `superseded` report code ships in the first release.
12. An equality query on a space with untokened rows fails with
    `conversion-pending`, and that space is swept first.
13. In-place encrypt conversions of a nullable plaintext column are refused;
    such columns use the copy-to-new-table recipe (§5.1). Conversions clear an
    encrypted cell to the cleared marker, not to the empty byte string, which
    keeps meaning "never written".
14. Equality queries on a converting column follow source-present
    precedence and index the source rows only for identity conversions or a
    declared bounded `reverse` mapping; otherwise they fail with
    `conversion-pending` until the space's conversion is done (§3.3).
