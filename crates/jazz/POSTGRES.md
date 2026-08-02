# Read-only PostgreSQL interface

Jazz can expose one app through a PostgreSQL-compatible, read-only endpoint.
The endpoint is disabled by default, binds only to `127.0.0.1`, and uses a
dedicated read-only database secret. It is available on core servers only
because an edge may hold a partial, query-scoped cache.

Start it with an app ID, PostgreSQL secret, and PostgreSQL port:

```sh
JAZZ_ADMIN_SECRET='replace-admin-me' \
JAZZ_POSTGRES_SECRET='replace-me' \
  jazz-tools server 00000000-0000-0000-0000-000000000001 \
  --postgres-port 5433 \
  --data-dir ./data
```

The server uses persistent RocksDB storage in `./data` by default. From this
repository, build and run the same command with:

```sh
JAZZ_ADMIN_SECRET='replace-admin-me' \
JAZZ_POSTGRES_SECRET='replace-me' \
  cargo run -p jazz --features cli --bin jazz-tools -- \
  server 00000000-0000-0000-0000-000000000001 \
  --postgres-port 5433 \
  --data-dir ./data
```

Connect with user `jazz`, the PostgreSQL secret as password, and the app ID as
the database name. Supplying the password separately avoids URI-escaping
problems; load `PGPASSWORD` from a secret manager or prompt in production:

```sh
PGPASSWORD='replace-me' \
  psql 'postgresql://jazz@127.0.0.1:5433/00000000-0000-0000-0000-000000000001?sslmode=disable'
```

For drivers that require one connection string, the equivalent URL is
`postgresql://jazz:replace-me@127.0.0.1:5433/00000000-0000-0000-0000-000000000001?sslmode=disable`.
Percent-encode reserved URI characters in an embedded password.

The backend, admin, and PostgreSQL secrets are intentionally separate. Neither
`JAZZ_ADMIN_SECRET` nor `JAZZ_BACKEND_SECRET` can authenticate this endpoint.
Publish the app schema with the admin secret through the inspector link printed
by the server, then use only `JAZZ_POSTGRES_SECRET` for database connections. A
fresh app lists its database immediately but has no application tables until a
schema is published.

The endpoint intentionally has no TLS because it cannot bind beyond loopback.
It uses PostgreSQL cleartext-password authentication, so the dedicated secret
is sent unencrypted on that loopback connection; it is not SCRAM. Do not proxy
it onto a public interface. Use an authenticated encrypted tunnel for remote
administration.

## Supported reads

The interface supports both PostgreSQL simple and extended/prepared query
protocols. The SQL subset is deliberately strict:

- one-table `SELECT` from `public`, with column projection or `*` and a
  required `LIMIT` (maximum 10,000);
- `WHERE` comparisons on PostgreSQL-native scalar columns, `AND`, `OR`, `NOT`,
  `IS NULL`, and `IN`;
- column `ORDER BY`, `LIMIT`, and `OFFSET` (maximum 10,000 each); nullable,
  large-value, and surrogate-text columns are not orderable yet;
- up to 1,024 `$n` parameters in filters, `LIMIT`, and `OFFSET`; decoded
  parameter data is capped at 4 MiB and expanded Jazz bindings at 8 MiB;
- `version()`, `current_database()`, `current_schema()`, `current_user`, common
  `SHOW` probes, and transaction framing commands;
- explicit virtual catalogue reads from `pg_catalog.pg_database`,
  `information_schema.tables`, and `information_schema.columns`.

A simple-protocol batch may contain at most one application-table `SELECT`;
send additional table reads as separate queries. Catalogue and session-probe
batches remain supported, with at most four row-producing statements per
batch.

Examples:

```sql
SELECT datname FROM pg_catalog.pg_database;

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'documents'
ORDER BY ordinal_position;

SELECT id, title, created_at
FROM documents
WHERE team_id = 'team-a'
ORDER BY created_at DESC
LIMIT 100 OFFSET 0;

-- Preferred beyond the bounded OFFSET range. Bind the final row of the
-- previous page as $2/$3.
SELECT id, title, created_at
FROM documents
WHERE team_id = $1
  AND (created_at < $2 OR (created_at = $2 AND id < $3))
ORDER BY created_at DESC, id DESC
LIMIT 100;
```

All application-table reads execute as the Jazz system/admin identity through
the server's existing database-owner thread and storage engine. SQL writes,
joins, aggregates, subqueries, and unsupported syntax return an error.
The public `id` column is the Jazz row UUID; treat `id` as reserved in app
schemas. Unsigned 64-bit integers are exposed as lossless decimal text, while
tuple and array values are exposed as stable JSON text.
Unsigned 64-bit, enum, tuple, and array columns are currently projection-only:
filtering or ordering their text representations is rejected because Jazz's
native comparison order would not match PostgreSQL text semantics. `IS NULL`
remains supported.

`psql` metacommands such as `\l`, `\dt`, and `\d`, plus GUI/ORM discovery,
use broader `pg_catalog` queries that are not implemented yet; use the
explicit catalogue queries above. Session `SET` commands and cursor/fetch-size
workflows are also unsupported.
Filter parameters must be non-`NULL`; use literal `IS NULL` or `IS NOT NULL`
syntax so SQL three-valued logic remains explicit. Transaction framing accepts
plain `BEGIN`/`START TRANSACTION`, `COMMIT`, and `ROLLBACK`; isolation/access
modes, savepoints, modifiers, and chaining are rejected.

## Pagination performance

Application-table reads require `LIMIT`; `OFFSET` is also capped at 10,000, and
materialized and encoded PostgreSQL results are capped at 16 MiB per response.
PostgreSQL database jobs are single-flight on the existing owner thread. Up to four
encoded `SELECT` responses may remain in flight; further reads fail fast until
a client consumes or closes an existing result, so one slow client does not
block all database work. Each connection may retain at most 64 prepared
statements and 64 portals, with additional byte limits on their SQL and bound
parameters. The listener accepts at most 64 simultaneous connections. SQL
text is capped at 64 KiB and also has token and nesting limits. PostgreSQL
cancellation requests cancel queued/asynchronous interface work; a synchronous
owner-thread scan that has already started may finish in the background.

These bounds prevent accidental unbounded response materialization, but the
current Jazz query engine still applies general ordering and pagination after
candidate-row materialization. A bounded page can therefore scan more rows
than it returns. Prefer indexed, selective filters and keyset pagination until
Jazz has a bounded ordered cursor.
