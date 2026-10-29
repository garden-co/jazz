# Server host tables in the store file (#4278)

> **Storage format 1, not yet shipped.** This adds tables to the format-1
> file; `STORAGE_FORMAT` stays 1. Earlier main builds kept the same state in
> side files (`catalogue.sqlite`, `accounts.sqlite`, `jazz.catalogue.sqlite`);
> no release wrote them, so a server neither reads nor refuses them (data
> dirs from unreleased builds are not supported). alpha.59 data dirs
> (`*.rocksdb`) are still refused until migrated.

A Jazz server keeps each app's state in one SQLite file, `jazz.sqlite` in its
data directory: the rows (unchanged, see the format-1 storage corpus) and the
server's own state as **host tables**. Citations are `path` plus symbol.

## Layout

A host table is `_jazz_host_<name>`:

```sql
CREATE TABLE _jazz_host_<name> (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID
```

- `<name>` is lowercase ASCII letters, digits and `_`
  (`crates/jazz/layers/db/src/host.rs` `HostTable::new`).
- A host table is created by its first write, inside that write's savepoint;
  a file no host wrote has none, and a read of a missing table finds nothing
  (`host.rs` `exists`, `create`).
- Every `_jazz*` table is Jazz's: application-table discovery skips them
  (`crates/jazz/layers/db/src/db.rs` `Db::open_on`), so host tables never
  reach an application schema, a sync peer or a client file.
- Each write is one top-level `SAVEPOINT host_write`, committed on release,
  on the store's connection, which the server opens with
  `synchronous = FULL` and WAL (`crates/jazz-server/src/runtime/mod.rs`
  `Source::open`): a returned write is durable.

The server keeps one host table. Its key names its encoding version.

| Table                                      | Key                                                                                                 | Value                     |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------- | ------------------------- |
| `_jazz_host_accounts` (`kv.rs` `ACCOUNTS`) | `account-command:v1:` + revision as u64 big-endian, dense from 0 (`account_store.rs` `command_key`) | postcard `AccountCommand` |

The account journal refuses a gap or an undecodable command, and the
registry stays unavailable.

The deploy catalogue is not a host table. Schemas, lenses and every
permissions revision are the store's own catalogue tables (`_jazz_schemas`,
`_jazz_lenses`, `_jazz_permissions`, beside `_jazz_versions` and
`_jazz_names`; `crates/jazz/layers/db/src/catalogue.rs`, SPEC ch. 10
§10.2), so the runtime and the server read one source. The server writes
them through the store's thread (`crates/jazz-server/src/server/catalogue_storage.rs`
`CatalogueStorage::put`, `HostTables::catalogue_put`) and a deploy admits
and activates in the same transaction (`Db::apply_catalogue`); it reads them
on the read-only connection (`HostReader::catalogue`) and rebuilds its HTTP
index from the rows (`catalogue.rs` `CatalogueIndex::from_rows`). There is
no catalogue log and no runtime snapshot: earlier builds of format 1 kept
them as `_jazz_host_catalogue` (`JCAT` entries) and
`_jazz_host_runtime_catalogue` (a postcard `CatalogueSnapshot`); no release
wrote them and nothing reads them now. A definition the server cannot parse
fails startup.

## Pinned evidence

- `crates/jazz-server/fixtures/server-data-dir-v1/` (README there): a
  catalogue-driven server's `jazz.sqlite` after two deployments (one with a
  migration), two rows and one registered account.
  `server::data_dir_corpus::pinned_server_data_dir_reopens_and_accepts_current_writes`
  reopens it and checks the deployments, migration graph, permissions,
  Alice's pinned account and both rows, then writes a deployment, an account
  and a row, and reopens again. `dev/gates/storage-compat.sh` runs it by
  exact name.
- `crates/jazz/tests/catalogue_rows.rs`: a deploy reads back as published,
  a reopened store admits from its own rows, a failed change leaves nothing,
  and every permissions revision is kept.

## Changing it

Until alpha.60 ships, re-run the producer and re-pin. Afterwards a rename of
a host table or a key, a new value encoding, or a change to how tables are
created needs a new key version (or a `STORAGE_FORMAT` bump) read beside the
old one, and a new corpus beside this one: a server must never open a pinned
file and find an empty table where state was.

## Concurrency (not durable, for reviewers)

One thread owns the store's connection and runs both host writes and the
runtime (the Durable Object model: one writer per file, no `SQLITE_BUSY`).
Host writes run ahead of queued runtime work, in tick-slice-sized rounds, so
they wait for at most one runtime job or tick slice. Host reads (a login's
journal fence, catalogue reads) use a second, read-only connection and never
wait for the runtime; they see every write that returned before they began.
If the runtime panics, the thread closes its sessions, reopens the file and
restarts the runtime; a second panic within a minute leaves it stopped and
`/health` unhealthy, with the host tables still served.
