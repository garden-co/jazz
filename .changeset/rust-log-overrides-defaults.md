---
"jazz-tools": patch
---

`RUST_LOG` now sets the level of any target it names in `createJazz` servers and the `jazz-tools` CLI. `RUST_LOG=jazz=debug` used to stay at info, because the built-in `jazz=info` default overrode it.
