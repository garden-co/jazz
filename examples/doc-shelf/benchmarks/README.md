# DocShelf benchmark

A team's markdown document library where most bodies are long: 73 of the 97
documents are 80 to 240 KiB and stored as chunked large values, the rest are
short inline notes, and a quarter of the long documents start from the same
6 KiB meeting-notes template, so they share a sort prefix. `title` and `body`
are indexed.

`benches/walltime.rs` measures what a writer notices:

- `doc_shelf_open_shelf`: the first 50 titles, bodies not projected.
- `doc_shelf_search_bodies`: `contains(body, …)` over every body.
- `doc_shelf_find_duplicates`: `body = <a pasted 230 KiB document>`.
- `doc_shelf_first_by_body`: the first 20 documents ordered by body, cutting
  inside the template documents' shared prefix.
- `doc_shelf_type_word`: appending a word to an open long document until its
  live editor view shows it.
- `doc_shelf_import_1mb`: inserting a 1 MiB document.
- `doc_shelf_sync_new_device`: a new device syncing the whole shelf over an
  in-memory byte link.

`doc-shelf-footprint` prints the size of a seeded file store.

```sh
cargo test -p jazz-example-doc-shelf-benchmark
cargo bench -p jazz-example-doc-shelf-benchmark --bench walltime
cargo run --release -p jazz-example-doc-shelf-benchmark --bin doc-shelf-footprint
```
