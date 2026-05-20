---
"jazz-tools": patch
"create-jazz": patch
---

Drop Node.js 20 support. Minimum is now Node.js 22.12 (Jod LTS). `engines.node` is set to `>=22.12` on `jazz-tools` and `create-jazz`; consumers on Node 20 will see an `EBADENGINE` warning (npm/pnpm) or a hard install failure (Yarn).
