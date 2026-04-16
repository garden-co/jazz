---
"create-jazz": patch
---

Accept a `--starter <name>` CLI argument to choose between scaffolds. Adds `next-localfirst` alongside the existing `next-betterauth` starter; defaults to `next-betterauth` when omitted; prompts interactively when no argument is passed on a TTY. Dependency resolution now falls back to `crates/<name>/package.json` when a workspace package is not found under `packages/`. The local-fetch copy path also filters out dev artefacts (`node_modules`, `.next`, `.jazz`, `.turbo`, `.env.local`) so locally-scaffolded projects never inherit a contributor's generated secrets.
