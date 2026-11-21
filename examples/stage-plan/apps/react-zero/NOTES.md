# StagePlan on Rocicorp Zero

## Versions

- `@rocicorp/zero` 1.12.0 (latest on 2026-11-14), `zero-cache` from the same package, run with
  `NODE_ENV=production` (not `zero-cache-dev`)
- Postgres 18.6 (Ubuntu apt package), `wal_level = logical`, local, same machine
- App server: Hono 4.10 on `@hono/node-server`, run with `tsx`; `jose` for the session JWT; `pg`
  through Zero's `zeroNodePg` adapter
- React 19.2.4, Vite 8.0.1, zod 4 (validators for queries and mutators), same UI packages as the
  Jazz copy

## Machine setup (once)

Postgres from apt (Ubuntu 26.04 ships PostgreSQL 18; the package's postinstall runs `initdb` and
creates the `18/main` cluster). Zero needs `wal_level = logical`, and the apps connect as
`postgres:pass@127.0.0.1:5432`:

```sh
sudo apt-get update && sudo apt-get install -y postgresql
# Only if no cluster exists yet (pg_lsclusters is empty): sudo pg_createcluster 18 main --start
sudo sed -i "s/^#\?wal_level.*/wal_level = logical/" /etc/postgresql/18/main/postgresql.conf
sudo service postgresql restart
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'pass';" -c "SHOW wal_level;"
```

Everything else is per app and automatic: `harness.mjs` runs `pnpm install --frozen-lockfile`
when `node_modules` is missing, starts Postgres if `pg_isready` fails, checks `wal_level`, and
(re)creates the app's database in `seed()`. Shared code: `harness/adapters/zero-local.mjs`.

Playwright's Chromium on Ubuntu 26.04: `PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64 pnpm exec
playwright install chromium` in `harness/`, plus `sudo apt-get install -y libnss3 libnspr4
libatk1.0-0t64 libatk-bridge2.0-0t64 libatspi2.0-0t64 libxcomposite1 libxdamage1 libxrandr2 libgbm1
libxkbcommon0 libpango-1.0-0 libcairo2 libasound2t64 libcups2t64`.

## Layout (as in Zero's docs and the `hello-zero` starter)

- `db/001_init.sql`: the Postgres schema; `schema.ts`: the Zero schema over it
- `queries.ts`: named queries (`defineQueries`); the read permissions are their filters, resolved
  on the server through `/api/zero/query` with the caller's context
- `mutators.ts`: custom mutators (`defineMutators`), run optimistically on the client and again on
  the server through `/api/zero/mutate`; the write permissions are checks in them that run on the
  server (`tx.location === "server"`), where reads see all data
- `server/index.ts`: anonymous sign-in, the two Zero endpoints, and the built app
- `src/`: the Jazz UI, with `useAll`/`useOne`/`db.*` swapped for `useQuery`/`zero.mutate`
  (`src/model/actions.ts` keeps the Jazz action names and signatures)

## Auth

Anonymous guests, the way `hello-zero` does it: `GET /api/login` issues an HS256 JWT in a `jwt`
cookie (`SameSite=Lax`) whose `sub` is a fresh random account id. The client reads the `sub` for
Zero's `userID`/`context`; zero-cache forwards the cookie (`ZERO_*_FORWARD_COOKIES=true`) and the
endpoints verify it. zero-cache runs on the page's own host (`127.0.0.1:4848`), so the browser
sends it the cookie. A guest's crew profile id is their account id.

## Behaviour differences and approximations

- **Notes merge.** Jazz merges concurrent edits to a task's notes (`merge("text")`); Zero has no
  text CRDT, so the last write wins.
- **Offline.** Zero does not take writes while disconnected and has no API to pause syncing.
  The top nav's "Sync" switch is kept but only shows whether Zero is connected (disabled).
- **Invite check.** Jazz carries the invite code on the membership row for its insert policy, then
  clears it. Here the `showCrew.join` mutator checks the code against `showInvites` on the server,
  so `showCrew` has no `inviteCode` column.
- **Chief repair / partial demo show.** Jazz repairs a chief membership or invite the server
  rejected, and resumes a partly rejected demo show. A Zero mutator is one transaction, so the
  show, membership and invite (and the demo board) land or fail together; `ensureChiefSetup` is
  gone and "Save the rest" re-runs the whole demo show.
- **Delete button.** Jazz asks `db.canDelete`; here the dialog shows it to the chief or the task's
  creator (`tasks.createdBy`), and the server checks the same rule.
- **Timestamps.** Jazz's `$createdAt`/`$updatedAt` are explicit `createdAt`/`updatedAt` columns
  (epoch ms), set by the mutators (the server's clock wins).
- **New-account check.** Like Jazz's wait for the server's copy of the profile, the bootstrap
  waits for `crew.mine` to be `complete` before deciding an account is new.
- Not ported (unused by the UI): show delete, comment delete.

## Harness

- `seed()` resets the `stage_plan_zero` database (dropping zero-cache's replication slots and its
  SQLite replica), applies `db/*.sql` and bulk-inserts the fixture as the seed chief; returns
  `{ showId, code }` of show-0 (also saved to `.bench-ctx.json` for `--no-seed`).
- `start()` builds with Vite, then serves the app server on `:4310` (built app + endpoints) and
  zero-cache (`NODE_ENV=production`) on `:4848`. `rttUrl` is zero-cache's `/`.

## Things that didn't work / caveats

- zero-cache logs that its `postgres.js` lacks Zero's COPY patch (the Docker image has it); initial
  sync of this small dataset was unaffected.
- A fresh replica logs a harmless `no such table: _zero.tableMetadata` once during initial sync.

## Permission checks

`pnpm exec tsx scripts/check-permissions.ts` (after `pnpm seed`) runs the server mutators and
queries as different users against the seeded database and checks the README's "Who can do what"
(outsiders, members, the creator or chief). It writes test rows, so seed again before a benchmark
run.
