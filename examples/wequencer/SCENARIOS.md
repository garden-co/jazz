# Wequencer scenarios

These synthetic, deterministic profiles give the sequencer a shared contract
between its UI, benchmarks, and later topology E2E tests. The first native
benchmark implements the local `grid-read`, `subscribed-pad-edit`, and
`editor-burst` operations below. The networked profiles are deliberately
specified here before their runner exists; they are not claimed as coverage.

| Profile                 | Topology                              | Scale                            | Operation                                                                            | Expected outcome                                                                                                    |
| ----------------------- | ------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `smoke-grid`            | one in-memory client                  | 4 tracks × 16 steps              | create, read ordered pads, toggle one pad                                            | exact position order; the write becomes locally visible                                                             |
| `subscription-hotspot`  | two browser contexts via SharedWorker | 16 × 64; 8 subscribers per track | 20 pad edits/sec for 30s, biased to 8 shared steps                                   | every subscriber converges on the ordered active window; no duplicate rows                                          |
| `partition-reconnect`   | two clients ↔ edge ↔ global           | 16 × 64; two editors             | partition for 60s; each editor toggles disjoint and overlapping pads, then reconnect | all settled pads converge under normal Jazz merge rules; denied writes surface as rejected rather than disappearing |
| `presence-expiry`       | two clients ↔ edge                    | 16 × 64                          | heartbeat every 5s; suspend one client for 30s; resume                               | presence is advisory only, may become stale, and never grants write access                                          |
| `transport-observation` | two browser contexts                  | 16 × 64                          | emit 10 transport observations/sec for 10s while pads change                         | ordered pad subscriptions remain complete; transport is convergent UI state, not audio-clock correctness            |
| `permission-roles`      | owner, editor, viewer                 | one shared session               | editor changes pads; viewer attempts the same; owner changes membership              | editor write settles; viewer write is rejected; visibility follows membership after refresh/reconnect               |

All profiles use fixture version `wequencer-v1` and a fixed seed when a runner
is added. This keeps a topology failure reducible to a public core regression,
then to the same app scenario, per the examples program.
