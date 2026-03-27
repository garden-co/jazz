# Storage Backend Alternatives

## What

Find better storage backends to replace Fjall, potentially using different engines for server and mobile since their constraints differ.

## Why

Fjall is a risky long-term bet. It was adopted primarily to have something that works equally well on servers and mobile apps, but the server and mobile constraints are different — we can pick the best option for each.

## Rough appetite

big

## Notes

- Possible candidates: RocksDB for the server, SurrealKV for React Native
- Need to validate any mobile candidate on RN (build toolchain, performance, stability)
- Migration path from Fjall needs consideration
