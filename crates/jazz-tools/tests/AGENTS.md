# Test Guidelines

## No mocks

All tests are integration tests against a real `TestingServer`. No mocked transports, no stubbed storage. If a behavior can't be exercised end-to-end, it's not tested here.

## Document each test

Every test function must have a doc comment (`///`) that states:

1. What contract or behavior it exercises
2. The actors involved (use human names: `alice`, `bob`, `mallory`)
3. An ASCII flow sketch when the causal order is non-trivial

Document with comments any non-obvious test instruction.

### ASCII sketch conventions

```
writer ──insert──► server ──broadcast──► subscriber
                      │
                      └── policy check ──✗── intruder
```
