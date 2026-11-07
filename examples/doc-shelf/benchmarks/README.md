# DocShelf benchmark

A team's markdown document library where most bodies are long: 73 of the 97
documents are 80 to 240 KiB and stored as chunked large values, the rest are
short inline notes, and a quarter of the long documents start from the same
6 KiB meeting-notes template, so they share a long common prefix. `title` and `body`
are indexed.

`benches/walltime.rs` measures what a writer notices. Queries never read a
chunked body's content (SPEC 19.5), so the body workloads use the shapes that
still answer:

- `doc_shelf_open_shelf`: the first 50 titles, bodies not projected.
- `doc_shelf_search_bodies`: `contains(body, …)` over every body; it matches
  only inline bodies, so of the four documents holding the phrase it finds
  the one short note.
- `doc_shelf_find_duplicates`: the most repeated body, by `count()` grouped on
  `body` (equal bodies written whole share one cell), read back in full.
  Looking the document up by a `body = <230 KiB literal>` filter is refused
  as not supported yet.
- `doc_shelf_first_by_body`: the first 20 documents ordered by body, which
  are chunked bodies (they sort before every inline one).
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
