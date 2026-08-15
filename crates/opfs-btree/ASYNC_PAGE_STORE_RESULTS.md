# Async page-store microbenchmark (WIP)

Three fixed-seed Chromium worker repeats, 200 keys, 4KiB pages, 3-page cache,
inline values only. Operations/s medians (K/s):

| value | store | seq put | random put | seq get | random get | 90/10 mixed | range | checkpoint+reopen/s |
| ----: | ----- | ------: | ---------: | ------: | ---------: | ----------: | ----: | ------------------: |
|   32B | IDB   |   111.1 |      117.6 |   222.2 |      181.8 |       200.0 |  31.2 |               454.5 |
|   32B | OPFS  |   111.1 |      125.0 |   222.2 |      222.2 |       181.8 |  30.8 |               238.1 |
|  256B | IDB   |   142.9 |      142.9 |   125.0 |      117.6 |       117.6 |   6.6 |               204.1 |
|  256B | OPFS  |   142.9 |      153.8 |   125.0 |      125.0 |       117.6 |   6.4 |                93.5 |

Run: `pnpm --dir crates/opfs-btree run test:wasm:async-page-stores`.
This is WIP microbenchmark data, not production or end-to-end performance. The
async tree currently excludes deletes/rebalance and overflow values; both stores
use identical Rust page codec, 3-page cache, split logic and workload driver.
