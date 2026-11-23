# E2EE schema changes: what the deploy enforces

What Jazz checks when the schema of an encrypted app changes, with the app
files (`jazz.app.json`, `jazz.permissions.json`) as the source, and how an
app encrypts data it already stores. References: the
[protocol](E2EE_PROTOCOL.md) ("spec §n"), the
[core hooks note](E2EE_CORE_HOOKS.md) ("H1"–"H11"), the
[crypto format](E2EE_CRYPTO_FORMAT.md) and
[lenses and migrations](10_lenses_migrations.md) ("ch. 10").

## 1. What these rules rest on

- **The server cannot transform ciphertext.** Any change to stored ciphertext
  or equality tokens is a write by a client that holds the space key.
- **What a cell is bound to.** The cell-record context (spec §4.1) binds the
  shared domain `json([appId, env])`, the scope table's catalogue identity,
  the space identifier, the table and column catalogue identities, the row,
  the epoch, and `T`, the canonical JSON of the logical plaintext type
  (including nullability, enum variants and JSON schema). Equality tokens bind
  the same identities without the row. Changing the domain, the table or
  column identity, the row or `T` makes old cells `invalid-ciphertext`.
  Changing the space is different: a cell that authenticates under a held key
  of another space is read, returned and reported `stale-space`, and one that
  does not is `invalid-ciphertext` once the row's space chain shows its epoch
  is not one of its own (spec §4.3). The deploy refuses a space or scope
  change anyway (§3.2).
- **Catalogue identities follow stored names** (ch. 10 §10.7). A rename lens
  keeps the stored table or column, and so its identity. A column whose stored
  value type changes, or which a lens adds, is a new stored column with a new
  identity.
- **The `encryption` entry is outside the schema hash** (H1). Editing an
  entry changes no stored column, so the deploy checks entries itself (§2,
  §3): without that, a type edit would deploy and every existing cell would
  then fail to authenticate.
- **History is never rewritten** (ch. 10 §10.1). Plaintext written before a
  column existed in encrypted form stays in row history, on replicas that
  synced it and in backups. Read-access eviction is not redaction
  (`INV-RLS-6`).
- **Encrypted and tag columns are non-null `bytes`** (spec §1.3). A null
  plaintext is encrypted like any other value. The empty byte string is never
  ciphertext: it means "never written" and reads as the declared default
  (when stored rather than lens-supplied it is also reported `empty-cell`).
- **Writes cannot move a row between spaces** (spec §5.1), and a write needs a
  held key for the row's space. Creating a space needs Jazz read and update
  permission on the space row (spec R15, §3.2); the client does not evaluate
  it and creates optimistically, in the transaction of the first encrypted
  write that needs the space.
- **Keys go to members only** (spec §1.3, R13). A space table's read rule
  must come from the members table (§4).

## 2. Declarations per schema version

- Every schema version records its tables' `encryption` entries: each
  `history` version in `jazz.app.json` carries them, as the current `tables`
  do, and the writer keeps them when it moves a version into `history`.
- Loaders return the declarations of every shipped version.
- `generate`, `validate` and the deploy check every migration against the
  declarations of its two versions (§3). The deploy refuses a change even
  when the stored schema is unchanged, as it does for a plaintext type
  change.
- The entries add nothing to the schema hash (H1); the deploy request carries
  every version's declarations so the server can check them.
- **Declarations are fixed per version.** A server keeps the declarations a
  version was first deployed with in a host table of its store. A redeploy of
  the same schema hash must send the same declarations, even for an enum
  append. For a new hash the server checks each encrypted column against
  every declaration it holds for the same stored table and column. Renames
  preserve this identity, and different hashes can share it even without an
  explicit migration.
- **Kept declarations commit with the catalogue.** A deployment that changes
  the catalogue keeps its first-declared versions' declarations in the same
  transaction, and a deployment that names declarations different from the
  kept ones for a version is refused before anything is live, however many
  instances share the store. The transaction belongs to one call and ends
  with it (a Durable Object core holds none open between calls).
- **Known limits: hosts that keep no declarations.**
  - A host whose deploy path takes no declarations (the Durable Object host)
    refuses every schema that has Jazz's system tables, with "has Jazz's
    system tables but declares no encryption", so an encrypted app cannot
    deploy there.
  - A server running without a store (a runtime only) has no account
    registry, which an encrypted app needs, so it refuses every deploy that
    brings Jazz's system tables, with "this app encrypts columns, which needs
    the account registry of an app's store", before anything is applied.

## 3. The deploy's rules on encrypted columns

### 3.1 Each version's entries

- An entry takes `space`, `scope`, `columns` and `equality`. A column is a
  type string, or an object with `type` and, optionally, `default` and
  `sealedType`. The reader refuses any other key, naming where it is and
  pointing to the copy recipe (§5); a raw deploy body with one is refused when
  it is decoded.
- The entry names a `space` (the column referencing the scope table, or `id`
  when the table is its own space table) and a `scope`, which is not one of
  Jazz's system tables. The space column and `id` stay plaintext: the server
  routes rows by them.
- An encrypted column is stored as required `bytes`, its stored default is
  the empty value, and it has no merge strategy (the server cannot merge
  ciphertext). Names starting with `__e2ee_` are Jazz's own.
- A required encrypted column that rows may predate declares its plaintext
  default in the entry, where it is public; otherwise it is optional.
- An equality column's tokens are stored in a required `bytes` column
  `__e2ee_eq_<column>`. Equality is declared only on the types the equality
  format encodes: boolean, integer, bigint, timestamp, double, text, bytes,
  uuid, enum and JSON columns, and arrays of any of them (also nested); row
  and transaction-ID columns, and arrays of them, are refused. JSON is matched
  by its exact stored text and arrays element by element, as Jazz's own
  equality matches them.
- Permissions that read an encrypted column's value are refused: the server
  sees only bytes. A space table's read rule must come from the members table
  (§4).

### 3.2 Between versions

Some rules compare each version with the version a migration leads from (the
direct migration edges): equality, renames of tag columns, a ciphertext
column whose declaration is dropped, a plaintext column that becomes
encrypted, and a new required column without a default. The others hold
across the whole history: a column's stored encoding, nullability, enum
variants and default, and the table's space and scope, are checked against
every stored declaration of the same stored table and column, whichever
version holds it.

| Change                                                     | Allowed?                                                     |
| ---------------------------------------------------------- | ------------------------------------------------------------ |
| New encrypted table                                        | Yes; spaces are created on first encrypted write             |
| New encrypted column on an encrypted table                 | Yes, by a lens `add` of a `bytes` column with default `""`   |
| Rename an encrypted column                                 | Yes, with its `__e2ee_eq_` column in the same migration      |
| Rename an encrypted table, its scope table or space column | Yes; identities are kept                                     |
| Drop equality tagging                                      | Yes, dropping the tag column in the same migration           |
| Append variants to an encrypted enum                       | Yes, pinning the original type as `sealedType`               |
| Any other type change, nullability included                | No: declare a new column, or copy into a new table (§5)      |
| Change a column's declared default                         | No                                                           |
| Change the space column or the scope table                 | No: copy into a new table (§5)                               |
| Make a plaintext column encrypted                          | No: copy into a new table (§5)                               |
| Stop declaring an encrypted column it keeps                | No: drop the column                                          |
| Add equality to a table that existed before                | No: declare equality when the table is created, or copy (§5) |

- **Types.** `T` is part of every cell's context, and the cell does not
  record which `T` it used, so each encrypted stored column keeps one `T` for
  life. A migration can append enum variants in place because encrypted enums
  store their variant names; the entry then pins the type the column was
  introduced with as `sealedType`. Each new declaration retains every
  previously declared list of variants for that stored column as a prefix,
  including variants added on another branch, so every version's variants lie
  on one append-only sequence: two versions that append different variants
  after the same prefix are refused, in whichever order they arrive and
  whether they are deployed together or separately. All declarations of a stored
  column have the same nullability: an optional value carries a presence
  byte, so changing nullability in either direction would leave old cells
  unreadable. A JSON schema change, even loosening a constraint, also needs a
  new column.
- **Defaults.** Rows stored before a column existed read its declared default,
  so another default would read them differently on clients of each version.
- **Space and scope.** The context binds the scope table's identity and the
  space identifier, so a change of `space` or `scope` that is not a rename
  would leave every cell unreadable.
- **Equality.** Rows older than a column have no equality token for it, and
  an index of tokens would miss them silently. So a column newly tagged in a
  table that existed in the previous version is refused, whether the
  migration adds the column or it was encrypted without equality. The deploy
  decides by the schema alone, never by looking at rows. Removing equality
  drops the tag column in the same migration: a token column left behind
  would be queried by older clients while nobody writes it. Tokens stay in
  history; they disclose equality only.
- **Ciphertext columns.** An encrypted column the new version keeps stays
  declared encrypted, or is dropped: a version that keeps the column but drops
  its declaration is refused. A column the earlier version stored as
  plaintext cannot be declared encrypted.

## 4. Members first

A space table's read rule must come from the members table once any table
encrypts columns in its spaces (spec R13). An app whose space table exists
with another read rule therefore moves in two deploys:

1. Declare an `encryption` entry with its `space` and `scope` but no encrypted
   columns (`"columns": {}`). This adds the E2EE system tables (under the
   schema-hash decision of the core hooks note) and makes the scope a space
   table, so sessions that may update a space row can write its member rows,
   while the read rule stays as it is (the deploy warns). The app then writes
   the member rows: for each space row, one row per account that should keep
   reading it, typically from its own membership data, from a client of an
   updater or a trusted backend. The creators of existing rows gain no right
   to add themselves: the creator clause holds only in the transaction that
   inserts the space row (spec R12). A space supports at most 1,000 member
   accounts (spec §3.8): a space row that more accounts read today needs a
   narrower readership or another space table. Nothing refuses member rows
   beyond the limit; the application keeps to it.
2. Change the space table's read rule to `exists __e2ee_members(...)` (spec
   §1.5) and add the encrypted table. Accounts without a member row lose read
   access to the space row at this deploy. From then on, a client that
   creates a space row adds its own first member row, in the same
   transaction, by the creator clause (spec §1.5, R12), and the space table's
   insert rule must not need a member row of the new row.

## 5. Recipe: encrypting data the app already stores

Jazz does not turn a plaintext column into an encrypted one. An app copies
the data into a new encrypted table instead (the developer guide is
`docs/content/docs/e2ee/encrypting-existing-data.mdx`):

1. Add the new table to `jazz.app.json` with its `encryption` entry, its
   encrypted columns and its `equality`, and `createTables` it in the
   migration (after §4 if its space table needs it); deploy and ship.
2. Freeze the old table: a permissions revision whose insert, update and
   delete rules on it are `false`, its read rule unchanged. Writes to that
   data pause until step 4. (The alternative, writing to both tables during
   the copy and reconciling them, changes every write path.)
3. In clients that hold a space's key, copy each old row of the space into
   the new table under the same row ID, with an upsert, so a second run
   overwrites rather than duplicates; nothing is skipped for having been
   copied before. When the space has no root yet, the first copy creates it
   (spec §3.2), which needs a session that may read and update the space
   row; the first encrypted rows and the space are accepted together. A space
   whose key holders are all offline waits. Verify, space by space, that the
   new table holds as many rows as the old one.
4. Ship the app version whose code, relations and permissions use the new
   table. Rows keep their IDs, so references to them still hold.
5. Drop the old table in a later migration.

**The old plaintext stays.** Copying protects the values from then on. The
old values remain in the old table's history, which no new version reads, on
replicas that synced them and in backups; dropping the table removes none of
these. The server operator removes the server's copy, with its backups,
outside Jazz. An app should treat that data as already disclosed.
