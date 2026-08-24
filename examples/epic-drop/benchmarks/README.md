# EpicDrop benchmark variant

This self-contained package models the same `folders` / `files` shape as the browser app. It streams a deterministic binary file from a bounded reader, lists the folder metadata, and reads a 64 KiB middle range without materializing the full file during download.

```bash
cargo test -p jazz-example-epic-drop-benchmark
cargo bench -p jazz-example-epic-drop-benchmark --bench file_operations
```
