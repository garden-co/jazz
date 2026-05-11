---
"create-jazz": patch
---

Add three TypeScript (no framework) starters: `ts-localfirst`, `ts-hybrid`, and `ts-betterauth`. Each mirrors its `react-*` counterpart but uses direct DOM manipulation inside the Jazz subscription callback, so users can see the underlying Jazz API without a UI framework in the way. The Hono + BetterAuth server in `ts-hybrid` and `ts-betterauth` is byte-identical to the corresponding `react-*` server (enforced by the parity script).

Also expose the existing `react-localfirst`, `react-hybrid`, and `react-betterauth` starters in the interactive picker as the "React (Vite)" framework option, and accept them via `--starter` (they were previously rejected as unknown).
