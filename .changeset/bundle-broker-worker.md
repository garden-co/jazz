---
"jazz-tools": patch
---

The browser broker SharedWorker is now shipped as self-contained, bundled ESM. It was previously unbundled with bare `../runtime/*.js` imports, and because its `new SharedWorker(...)` call is indirected past bundler worker-detection, Turbopack, webpack and Vite copied it verbatim and its imports 404'd on load — crashing every Jazz app under `next dev` / `next build` and `vite build` with "Browser broker SharedWorker failed to start". (`vite dev` masked it.) Fixing it in the package build covers all frameworks.
</content>
