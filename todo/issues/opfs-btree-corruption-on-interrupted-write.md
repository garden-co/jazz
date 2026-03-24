# OPFS B-tree corruption on interrupted write

## What

Reloading the page mid-write can corrupt the OPFS B-tree, producing "page id X out of bounds for total_pages Y" errors on next load.

## Where

`crates/opfs-btree/src/db.rs` — `checkpoint()` method and `read_page_raw_from_disk` / `read_page_run_from_disk` bounds checks.

## Steps to reproduce

1. Start writing data in a Jazz-powered app
2. Reload the page while writes are in progress
3. Reload again — storage error on load

## Expected

Graceful recovery or atomic checkpoint so partial writes don't corrupt the B-tree.

## Actual

`IoError("opfs-btree: corrupt data: page id 41127 out of bounds for total_pages 41116")`

## Priority

high

## Notes

Root cause: `checkpoint()` writes dirty pages and flushes, then writes the superblock with the updated `total_pages`. If the tab is closed between the flush and the superblock write, the on-disk pages reference IDs beyond the stale `total_pages` recorded in the superblock. This is a classic write-ordering / crash-recovery gap — the superblock update is not atomic with the page writes. A WAL or double-write superblock strategy would fix this.

See also: https://discord.com/channels/1139617727565271160/1480597988282994698
