# BandBinder benchmark variant

This native package duplicates BandBinder's workspace/page/block relationship
shape without importing the application. It measures a bounded ordered sibling
window and one recursive-page traversal step. Follow-up app E2E must exercise
real auth scopes, concurrent moves, offline replay, and topology faults.

```sh
cargo test -p jazz-example-band-binder-benchmark
cargo bench -p jazz-example-band-binder-benchmark --bench queries
```
