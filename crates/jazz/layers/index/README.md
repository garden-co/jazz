# `jazz-index`

Jazz's specialised index kinds ([SPEC 20](https://github.com/garden-co/jazz/blob/main/crates/jazz/SPEC/20_specialised_indexes.md)): the pure functions behind text, vector and geo search.

- **`text`**: the `jazz-text/1` analyzers (`jazz-text/1:en` with Porter2 stemming, `jazz-text/1:simple`), and each term's impact from its in-field frequency and field length.
- **`vector`**: `f32le/1` vector encoding, and cosine, L2 and dot scores.
- **`geo`**: points' `jazz-ix-geo-z62/1` cells and unit vectors, and the cell ranges and chord thresholds that cover a radius or a box.

Everything here is a function of its inputs, with no storage and no SQL. Every node (native, browser WASM, React Native, Durable Objects) therefore computes the same terms, impacts, cells and scores. The SQLite side (shadow tables, triggers, draining and query compilation) lives in `jazz-db`.

Each kind is a cargo feature (`text`, `vector`, `geo`, all on by default), so an embedding that doesn't need one can drop it to save WASM size.

This crate is published so that `jazz-db` and `jazz-core` can depend on it. Apps use it through `jazz-core` and `jazz-tools`, not directly.
