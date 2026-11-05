# migrate-alpha59

Moves an alpha.59 server data dir (RocksDB) to the current server format.
Self-hosters running alpha.59 use it to upgrade. It stays released: the
exporter and importer binaries stay attached to the release they shipped
with. (The npm `jazz-tools` carries no native binary, so the importer ships
next to the exporter rather than in it.)

## For self-hosters

1. Stop the alpha.59 server. Keep its data dir; nothing below writes to it.
2. Download `jazz-alpha59-export-<platform>` and
   `jazz-alpha59-import-<platform>` (Linux x64/arm64, macOS arm64) from the
   GitHub release you upgrade to, check them against
   `jazz-alpha59-export.sha256` and `jazz-alpha59-import.sha256`, and make
   them executable.
3. Export. `--work` is an empty scratch dir: the exporter reads a private
   copy of the data dir, never the original.

   ```sh
   jazz-alpha59-export --data-dir ./data --work ./export-work --out app.dump
   ```

4. Import into a new, empty dir, then verify:

   ```sh
   jazz-alpha59-import import --dump app.dump --data-dir ./data-new --app-id <app id>
   jazz-alpha59-import verify --dump app.dump --data-dir ./data-new --app-id <app id>
   ```

   `--app-id` must be the app id your app passes to `remote()`, exactly,
   case included: it names the `createJazz` identity of every returning
   user (see below).

5. Start the new server on `./data-new`. Keep `./data` until you are happy.

The dump holds all of the app's data: keep it where the data dir is, and
delete it when done.

## What the import does

The dump carries the runtime schemas and lenses, every admin catalogue
entry, the account journal, what alpha.59 answers for each principal's
login, what alpha.59 reads as each row's current state (its settled content
and deletion winners), then every stored row version in commit order with
its authors and timestamps. It ends with a BLAKE3 digest, which the import
checks before it builds anything.

- **Admin catalogue:** only the active schema and its permissions, as one
  fresh deployment, which publishes it as a bundle. Old schemas, migrations
  and permission revisions stay behind, so the store matches a client that
  ships only the current schema. Schema hashes are recomputed with the
  current engine's own hash function.
- **Accounts:** the journal replayed command by command.
- **Backend identity links:** apps moving to `createJazz` name users
  `(jazz:app:<appId>, userId)`. So that `authenticate` answering
  `{ userId: sub }` opens a returning user's old account, each admitted
  external identity `(iss, sub)` gets `(jazz:app:<appId>, sub)` linked to the
  same account, through the registry's own link commands. Built-in issuers
  (`urn:jazz:*`) are left alone, and revoked identities get no link. The
  registry grants every linked identity the right to manage the account's
  identities, so the import refuses an identity that lacks it (alpha.59
  never creates one). It also refuses, naming only issuers, when two issuers
  hold a subject on different accounts, revoked or not. Pass
  `--no-backend-identity-links` to skip this.
- **Rows:** every version, brought forward to the active schema along
  alpha.59's own runtime lenses (columns those lenses dropped stay dropped),
  then written through the public Db API as `WriteIdentity::Attribution` of
  its original author with its original timestamp. Each insert and update
  writes every column, absent cells as explicit nulls.

The import refuses to finish when its replay leaves any row in another state
than alpha.59 reads for it. A failed import removes what it built (a dir
that existed beforehand is kept, emptied).

`verify` checks the published bundle's schema and permissions, every login
answer, every backend identity link, and every row's deleted flag, cells
and created/updated provenance against what alpha.59 itself reads as the
row's current state, through the new server's own read paths. It prints row
ids, table and column names only, never values or authors.

## Not supported yet (fails loudly)

Branched rows, schemas published but never activated, rows with no lens
path to the active schema, TransformColumn lens ops (no shipped alpha.59
tool creates them; `dump-census` reports them under `unsupported_by_sqlite`),
versions not yet settled by the authority (`pending`), content written while
a row was deleted (alpha.59 keeps it under the deletion; the current engine
cannot), and transaction metadata, which the current engine does not have:
`--drop-transaction-metadata` imports those versions without it and counts
them. Verify does not yet compare history depth per row or permissions as
seen by each account.

## Layout

| piece                             | built                                | does                                                                   |
| --------------------------------- | ------------------------------------ | ---------------------------------------------------------------------- |
| `jazz-alpha59-export` (`export/`) | in a `v2.0.0-alpha.59` checkout      | copies the data dir, reads the copy with alpha.59's decoders, dumps it |
| `crates/jazz-alpha59-dump`        | in both                              | the dump format, plus `dump-show` and `dump-census`                    |
| `jazz-alpha59-import`             | in this checkout (`crates/jazz-cli`) | `import` builds a new data dir from a dump, `verify` checks it         |
| `produce/`                        | in the alpha.59 checkout             | the synthetic tenant `test.sh` migrates                                |

The exporter must link alpha.59 itself, and can't share a build with the
importer: the two pull different libsqlite3-sys versions, which both
declare `links = "sqlite3"`. `prepare.sh` checks out alpha.59 (or uses
`A59_DIR`), copies small read-only hook modules and the dump crate into it,
and adds the exporter and fixture to that checkout's workspace. The
`alpha.59 exporter` workflow does the same in CI, builds the importer from
the release's own tag, and attaches both to the release.

```sh
dev/migrate-alpha59/test.sh      # end to end on a synthetic tenant

# census: counts and lens-op kinds only, safe to share
jazz-alpha59-export --data-dir ./data --work <empty dir> --out c.dump --catalogue-only
dump-census c.dump
```

`dump-show` prints string prefixes, cell values and authors, so only run it
on synthetic dumps.
