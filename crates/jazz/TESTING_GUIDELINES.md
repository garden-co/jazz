# Jazz Testing Guidelines - Rust

- Prefer black-boxed integration tests that exercise public APIs over unit or
  white-box tests. When a lower-level test is genuinely needed, say why in the
  test every time.
- Build schemas, permissions and queries with the public builders, never with
  JSON-like definitions.
- Assert user-visible effects through public client APIs: query rows,
  subscription deltas, write settlement, visible row state.
- Fixtures isolate app ids, ports, storage and client state; tests run in
  parallel.
- Give each test a `///` doc comment stating the contract it exercises and the
  actors involved (human names: `alice`, `bob`, `mallory`), with an ASCII flow
  sketch when the causal order is non-trivial:

  ```
  writer ──insert──► server ──broadcast──► subscriber
                        │
                        └── policy check ──✗── intruder
  ```
