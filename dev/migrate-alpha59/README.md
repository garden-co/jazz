# migrate-alpha59

One-time tool that moves an alpha.59 server data dir (RocksDB) to the
SQLite engine's layout. **Delete this folder once the fleet runs alpha.60.**
Nothing here is part of the workspace or of any release build.

## How it works

Two binaries, because alpha.59 and the SQLite engine link different
`libsqlite3-sys` versions that both declare `links = "sqlite3"`:

| binary          | built in                 | does                                                                                        |
| --------------- | ------------------------ | ------------------------------------------------------------------------------------------- |
| `a59-export`    | jazz `v2.0.0-alpha.59`   | copies the data dir, reads the copy with alpha.59's own decoders, writes a dump             |
| `sqlite-import` | the SQLite engine commit | builds a new data dir from a dump through the new server's own code paths, and `verify`s it |

`prepare.sh` checks out each version (or uses `A59_DIR` / `A60_DIR`), copies
small hook modules into it to reach private internals, and adds the crate to
that checkout's workspace. The hooks only read (export) or call existing
server functions (import).

The dump (`dump/`) is version-neutral: runtime schemas and lenses, every
admin catalogue entry decoded to JSON, the account journal, what alpha.59
answers for each principal's login, what alpha.59 reads as each row's
current state (its settled content and deletion winners), then every stored
row version in commit order with its authors and timestamps. It ends with a
BLAKE3 digest, which the import checks before it builds anything. The
import also refuses to finish when its replay leaves any row in another
state than alpha.59 reads for it; `verify` then checks the result through
the new server's own read paths. A failed import removes what it built
(a dir that existed beforehand is kept, emptied).

Import:

- **Admin catalogue:** only the active schema and its permissions, as one
  fresh deployment, which publishes it as a bundle. Old schemas,
  migrations and permission revisions stay behind, so the store matches a
  client that ships only the current schema. Schema hashes are recomputed
  with the SQLite engine's own hash function.
- **Accounts:** the journal replayed command by command.
- **Backend identity links:** apps moving to `createJazz` name users
  `(jazz:backend, userId)`. So that `authenticate` answering
  `{ userId: sub }` opens a returning user's old account, each admitted
  external identity `(iss, sub)` gets `(jazz:backend, sub)` linked to the
  same account, through the registry's own link commands. Built-in issuers (`urn:jazz:*`, `jazz:*`) are left alone, and revoked
  identities get no link. The registry grants every linked identity the
  right to manage the account's identities, so the import refuses an
  identity that lacks it (alpha.59 never creates one). It also refuses,
  naming only issuers, when two issuers hold a subject on different
  accounts, revoked or not. Pass `--no-backend-identity-links` to skip this.
- **Rows:** every version, brought forward to the active schema along
  alpha.59's own runtime lenses (columns those lenses dropped stay dropped),
  then written through the public Db API as `WriteIdentity::Attribution` of
  its original author with its original timestamp. Each insert and update
  writes every column, absent cells as explicit nulls.

`sqlite-import verify` checks the published bundle's schema and
permissions, every login answer, every backend identity link, and every
row's deleted flag, cells and created/updated provenance against what
alpha.59 itself reads as the row's current state (not against the import's
own replay), and that no table holds rows alpha.59 lacks. It prints row ids,
table and column names only, never values or authors.

## Use

```sh
dev/migrate-alpha59/test.sh      # end to end on a synthetic tenant

# per tenant
a59-export --data-dir /app/data --work <empty dir> --out tenant.dump
sqlite-import --dump tenant.dump --data-dir <new dir> --app-id <app uuid>
sqlite-import verify --dump tenant.dump --data-dir <new dir> --app-id <app uuid>

# census: counts and lens-op kinds only, safe to leave the cluster
a59-export --data-dir /app/data --work <empty dir> --out c.dump --catalogue-only
dump-census c.dump
```

`--app-id` is the alpha.59 app's id: its guests' accounts are derived from
it. The import keeps it in `<new dir>/app-id`, so `createJazz` with
`sqlite({ path: "<new dir>" })` runs as that app without the app naming
the id.

Dumps hold customer data: keep them inside the cluster and never commit
them. `dump-show` prints string prefixes, cell values and authors, so only
run it on synthetic dumps.

## Not supported yet (fails loudly)

Branched rows, schemas published but never activated, rows with no lens
path to the active schema, TransformColumn lens ops (no shipped alpha.59
tool creates them; `dump-census` reports them under `unsupported_by_sqlite`),
versions not yet settled by the authority (`pending`), content written while
a row was deleted (alpha.59 keeps it under the deletion; the SQLite engine
cannot), and transaction metadata, which the SQLite engine does not have:
`--drop-transaction-metadata` imports those versions without it and counts
them.
Verify does not yet compare history depth per row or permissions as seen by
each account.
