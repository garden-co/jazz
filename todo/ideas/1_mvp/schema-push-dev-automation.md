# Schema & Permissions Push (Automatic in Dev)

## What

Push schema and permissions to the server automatically during development, via NAPI + a Vite plugin and a Next.js plugin. In dev mode this should happen transparently on startup or schema change, with no manual step required.

## Notes

- Scope is automatic schema and permissions push in dev, triggered on startup or schema change.
- Intended delivery surface is NAPI plus framework plugins for Vite and Next.js.
