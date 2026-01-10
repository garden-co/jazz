# Load balancing harness (SQLite + mixed big/small)

This is a runnable Node harness (not a `vitest` test) that:

- Seeds a **single SQLite DB** with **50% PDF FileStreams** and **50% small maps**
- Queries the SQLite DB for **all coValue ids + headers**
- Starts a local **sync server** using that same SQLite DB as storage
- Spawns **N worker threads** that generate mixed load with a configurable **file/map ratio**

## Setup

From the repo root:

```bash
pnpm -C tests/load-balancing install
```

Put a PDF in this folder (recommended):

- `tests/load-balancing/assets/sample.pdf`

## Seed

```bash
pnpm -C tests/load-balancing run seed -- --items 100 --pdf ./assets/sample.pdf
```

## Query ids + headers

```bash
pnpm -C tests/load-balancing run query
```

Optionally limit output rows:

```bash
pnpm -C tests/load-balancing run query -- --limit 50
```

## Run load (sync server + workers)

Fair mix (1 file then 1 map):

```bash
pnpm -C tests/load-balancing run run -- --workers 8 --durationMs 60000 --inflight 4 --mix 1f:1m
```

Biased mix (2 files then 1 map):

```bash
pnpm -C tests/load-balancing run run -- --workers 8 --durationMs 60000 --inflight 4 --mix 2f:1m
```

Optional: randomize ordering while keeping the ratio:

```bash
pnpm -C tests/load-balancing run run -- --workers 8 --durationMs 60000 --inflight 4 --mix 2f:1m --mixMode randomized
```

## One-shot (seed → query → run)

```bash
pnpm -C tests/load-balancing run all -- --items 100 --pdf ./assets/sample.pdf --workers 8 --durationMs 60000 --inflight 4 --mix 1f:1m
```

## Scaling guidance

- Increase `--items` gradually (e.g. 10 → 100 → 1_000).
- Keep `--inflight` small-ish (e.g. 2–8) to avoid over-saturating the event loop.
- Increase `--workers` to push concurrency; the harness reports aggregate ops/sec.

