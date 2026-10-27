# migrate-alpha59

One-time tool that moves an alpha.59 server data dir (RocksDB) to the
SQLite engine's layout. **Delete this folder once the fleet runs alpha.60.**
Nothing here is part of the workspace or of any release build.

## How it works

Two binaries, because alpha.59 and the SQLite engine link different
`libsqlite3-sys` versions that both declare `links = "sqlite3"`:

| binary | built in | does |
| --- | --- | --- |
| `a59-export` | jazz `v2.0.0-alpha.59` | copies the data dir, reads the copy with alpha.59's own decoders, writes a dump |
| `sqlite-import` | the SQLite engine commit | builds a new data dir from a dump through the new server's own code paths, and `verify`s it |

`prepare.sh` checks out each version (or uses `A59_DIR` / `A60_DIR`), copies
small hook modules into it to reach private internals, and adds the crate to
that checkout's workspace. The hooks only read (export) or call existing
server functions (import).

The dump (`dump/`) is version-neutral: runtime schemas and lenses, every
admin catalogue entry decoded to JSON, the account journal, what alpha.59
answers for each principal's login, then every stored row version in commit
order with its authors and timestamps. It ends with a BLAKE3 digest.

Import:

- **Admin catalogue:** the permissions history replayed as one
  deployment per bundle, oldest first (the code `POST /admin/deploy` runs).
  Content-addressed ids come out identical.
- **Accounts:** the journal replayed command by command.
- **Rows:** every version written through the public Db API, under the
  schema version it was written with, as `WriteIdentity::Attribution` of its
  original author with its original timestamp.

`sqlite-import verify` checks the permissions head, every login answer,
and every row's deleted flag, cells and created/updated provenance. It
prints row ids and column names only, never values or authors.

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

Dumps hold customer data: keep them inside the cluster and never commit
them.

## Not supported yet (fails loudly)

Branched rows, schemas published but never activated, and admin lens ops
other than add/remove/rename column and add/remove/rename table. Runtime
CopyColumn/TransformColumn lenses can't be created by any shipped alpha.59
tool; `dump-census` reports them under `unsupported_by_sqlite`. Verify does
not yet compare history depth per row or permissions as seen by each
account.
