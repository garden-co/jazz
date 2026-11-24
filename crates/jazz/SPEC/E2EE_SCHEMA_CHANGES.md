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
  of another space is read, returned and reported `stale-space`. One that does
  not is treated as a key the device lacks (`key-pending` while it fetches it,
  `key-unavailable` if it cannot) until the row's space chain shows that the
  cell's epoch is not one of its own; from then on it is `invalid-ciphertext`
  (spec §4.3). The deploy refuses a space or scope
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
- **Writes cannot move a row between spaces** (spec §5.1). The check runs
  in the writer's own runtime, against its local copy of the row
  (`write_space` in the client and keyless hooks, and the streamed-write
  check); the authority does not repeat it. A writer that has not loaded
  the row can therefore upsert it with another space column.
  - The upsert's generated values for the columns it omits apply only if
    the row is created, so an existing row keeps those cells, sealed for
    its old space.
  - The row's readers are now the new space's members. One that holds a
    key of the old space reads those cells, reported `stale-space`. Any
    other reader treats them as a missing key until the chain shows the
    epoch is foreign, then as `invalid-ciphertext` (above), and an equality
    filter by those columns misses the row.
  - What the new space's readers see: the row's plain columns, and the
    encrypted values the upsert supplied, which are sealed for the new
    space. The encrypted values it left out stay sealed for the old space
    and are not disclosed.
  - Only a session the application's update rule admits, or the
    application's backend, can write this.
  - Writing the original space back repairs it. The same check refuses
    that write from a writer whose copy shows the new space, so it comes
    from a writer that has not loaded the row. Values the moving upsert
    supplied are then written again, by a writer that holds the old
    space's key.

  Writing an
  encrypted value needs a held key for the row's space; an update that
  touches only plain columns needs none, and a backend that holds no keys
  writes plain columns only (it is refused any encrypted column). A client's
  insert also seals each encrypted column it leaves out that is optional
  (null) or declares a default, so such an insert needs the key.
  Creating a space needs Jazz read and update permission on the space row
  (spec R15, §3.2); the client does not evaluate it and creates
  optimistically, in the transaction of the first encrypted write that needs
  the space.

- **Keys go to members only** (spec §1.3, R13). A space table's read rule
  must come from the members table (§4).

## 2. Declarations per schema version

- Every schema version records its tables' `encryption` entries: each
  `history` version in `jazz.app.json` carries them, as the current `tables`
  do, and the writer keeps them when it moves a version into `history`.
- `systemTablesVersion` in `jazz.app.json` identifies the system tables for the
  current schema and independently for each schema in `history`. Omission
  means version 1. The loader restores each schema’s own system tables
  before hashing it, preserving the hashes and bytes of existing version-1
  files. Jazz supplies the additive system-table migration steps. The server
  accepts known versions and refuses downgrades or unknown versions. An
  unknown version requires a server upgrade. See the
  [versioned system table tests](../tests/system_table_upgrade.rs#L39-L152).
- What each loader returns:
  - `load` (the app files) and `load_backend` (generated bindings), which
    deploy or serve the app: the declarations of every shipped version.
  - `load_client_encrypted` (the app file) and `bound_declarations`
    (generated bindings), from which a client builds its encryption hooks:
    the current version's declarations only, or none when that version has
    no system tables.
  - `load_client`: schema versions and lenses, no declarations.
  - `load_client_current`: no declarations; it refuses an app whose current
    version has Jazz's system tables.
- `generate`, `validate` and the deploy check every migration against the
  declarations of its two versions (§3). The deploy refuses a change even
  when the stored schema is unchanged, as it does for a plaintext type
  change.
- **Known limit: the tooling sees only the app files.** `migrations create`,
  `generate` and `validate` run the deploy's own checks on what the files
  say, and the deploy has the final word. When the outcome depends on which
  versions a server has deployed, the tooling accepts and the deploy
  decides. For example, an index-only edit that shortens an encrypted enum
  list is a sibling change in the files, but the server treats it as
  narrowing a deployed descendant when that descendant is the only version
  with the same row shape, and refuses it. The tooling never refuses a
  change the deploy accepts.
- **Known limit: a version that declares no encryption entries keeps its
  inherited space tables.** Jazz derives its system-table policies from the
  nearest earlier declarations (see E2EE_CORE_HOOKS.md). Every space table
  they name must exist under its original name, even if the entries naming
  it have no encrypted columns. If any of those entries encrypts a column,
  the space table's read rule must come from the members table.
  An encrypted table can be dropped unless it is itself an inherited space
  table. To drop or rename a space table, declare encryption in the new
  version so its own entries govern. An entry with no encrypted columns is
  enough. Otherwise, keep the table under its original name. A version
  already deployed to a server without declarations can't gain them (declarations
  are fixed per version, below), so there the change needs a new version.
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

- An entry takes `space`, `scope`, `columns` and `equality`. Each value in
  `columns`, keyed by column name, is one of:
  - a type string: `string`, `int`, `bigint`, `float`, `boolean`,
    `timestamp`, `uuid` or `bytes`, with `?` appended for an optional column;
  - a type object with exactly one of `type` (a type string), `enum` (an
    array of variant names), `json` (`true`, or a JSON schema) and `array`
    (the element type), and optionally `optional` (`true` or `false`) and
    `default`;
  - a declaration `{ "type": …, "sealedType": …, "default": … }`, where
    `type` and `sealedType` are each a type string or a type object without
    `default`, and `sealedType` and `default` are optional.

  A type nested in `type` or `array` is a type string or an object with
  exactly one of `type`, `enum`, `json` and `array`. Any of these objects may
  also carry a string `$comment`; `columns` itself takes only column names.
  The reader refuses any other key, naming the key and where it is; a raw
  deploy body with one is refused when it is decoded.

- The entry names a `space` (the column referencing the scope table, or `id`
  when the table is its own space table) and a `scope`, which is not one of
  Jazz's system tables. The space column and `id` stay plaintext: the server
  routes rows by them.
- An encrypted column is stored as required `bytes`, its stored default is
  the empty value, and it has no merge strategy (the server cannot merge
  ciphertext). Names starting with `__e2ee_` are Jazz's own.
- A required encrypted column that rows may predate declares its plaintext
  default in the entry, where it is public; otherwise it is optional.
- A declared `default` is a value of the column's declared type: `null` only
  when the column is optional; for an enum, one of its variants; for JSON,
  the JSON value itself (an object default is written as an object, not as
  quoted text), which must satisfy the column's JSON schema if it has one;
  for an array, an array whose elements each fit the element type; and for
  any other type, a value of exactly that type. The deploy, `generate` and
  the loaders all check this. `sealedType` names only the stored type and
  takes no `default`: the default is declared beside it.
- Encrypted columns hold the types the type strings and objects above name.
  The deploy refuses, for the current version's declared and sealed types,
  any type a client can't seal or open: row and transaction-ID types,
  payload enums and catalogue scalar enums, arrays of them, and a JSON
  schema containing a number that isn't an integer (such as
  `"minimum": 0.5`), since the cell type encodes only integers.
- An equality column's tokens are stored in a required `bytes` column
  `__e2ee_eq_<column>`, whose stored default, if it declares one, is the
  empty byte string. Equality is declared only on the types the equality
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

| Change                                                     | Allowed?                                                                     |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------- |
| New encrypted table                                        | Yes; spaces are created on first encrypted write                             |
| New encrypted column on an encrypted table                 | Yes, by a lens `add` of a `bytes` column with default `""`                   |
| Rename an encrypted column                                 | Yes, with its `__e2ee_eq_` column in the same migration                      |
| Rename an encrypted table, its scope table or space column | Yes; identities are kept                                                     |
| Drop equality tagging                                      | Yes, dropping the tag column (`backwardsDefault` `""`) in the same migration |
| Append variants to an encrypted enum                       | Yes, pinning the original type as `sealedType`                               |
| Any other type change, nullability included                | No: declare a new column, or copy into a new table (§5)                      |
| Change a column's declared default                         | No                                                                           |
| Change the space column or the scope table                 | No: copy into a new table (§5)                                               |
| Make a plaintext column encrypted                          | No: copy into a new table (§5)                                               |
| Stop declaring an encrypted column it keeps                | No: drop the column                                                          |
| Add equality to a table that existed before                | No: declare equality when the table is created, or copy (§5)                 |

- **Types.** `T` is part of every cell's context, and the cell does not
  record which `T` it used, so each encrypted stored column keeps one `T` for
  life. A migration can append enum variants in place because encrypted enums
  store their variant names; the entry then pins the type the column was
  introduced with as `sealedType`. The variant lists of any two declarations
  of that stored column, including ones on another branch, lie on one
  append-only sequence: one is a prefix of the other. `[a]` beside `[a, b]`
  is accepted, and two versions that append different variants after the
  same prefix, such as `[a, b]` beside `[a, c]`, are refused, in whichever
  order they arrive and whether they are deployed together or separately. All
  declarations of a stored column have the same nullability: an optional
  value carries a presence byte, so changing nullability in either direction
  would leave old cells unreadable. A JSON schema change, even loosening a
  constraint, also needs a new column.
- **Defaults.** Rows stored before a column existed read its declared default,
  so another default would read them differently on clients of each version.
  Every declaration of a stored column therefore has the same default, or
  none in all of them; each must also be a value of its type (§3.1).
- **Space and scope.** The context binds the scope table's identity and the
  space identifier, so a change of `space` or `scope` that is not a rename
  would put every row in another space than the one its cells were sealed
  for. As §1 says, a device holding a key of the old space would still read
  such a cell, reported `stale-space`; on any other device it would read as
  a missing key until the space chain shows the epoch is not the space's,
  and as `invalid-ciphertext` from then on.
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
- **Lens values for ciphertext and token columns.** A migration's lens that
  adds an encrypted column or its `__e2ee_eq_` column gives it the empty byte
  string as its `default`, and a lens that drops one gives the empty byte
  string as its `backwardsDefault`: older clients read that column back
  through the lens, and only the empty value reads as never written.

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
   §1.5) and add the encrypted table. An account without a member row loses
   read access to the space row once its session is pinned to the bundle
   with this rule; a session pinned to an older bundle keeps the old rule
   until the deployment serving that bundle is retired
   ([lenses and migrations](10_lenses_migrations.md) `INV-LENS-7`,
   [topology](9_topology.md) §9.7). From then on, a client that
   creates a space row adds its own first member row, in the same
   transaction, by the creator clause (spec §1.5, R12), and the space table's
   insert rule must not need a member row of the new row.

## 5. Encrypting existing data

Jazz does not convert a plaintext column into an encrypted one. Follow the
[migration guide](../../../docs/content/docs/e2ee/encrypting-existing-data.mdx)
to copy it into a new encrypted table. The old table and every old reference
must remain unwritable throughout the final copy and check; stop every writer,
including sessions pinned to older bundles and backend jobs. A reference
cannot be retargeted in place, and a required reference cannot be made
optional by a migration, so add new reference columns and drop the old ones
only after switching.

**The old plaintext stays.** Dropping the old table does not erase its history,
replica copies or backups. The server operator must remove those outside Jazz;
treat the old data as already disclosed.
