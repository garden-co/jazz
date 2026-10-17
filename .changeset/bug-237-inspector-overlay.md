---
"jazz-tools": patch
---

Wire the Inspector overlay into the SvelteKit development integration and dispose managed runtime state when the plugin closes. The SvelteKit and Vite dev plugins now keep the managed Jazz server running across Vite dev-server restarts (`.env` or `vite.config.ts` changes), and stop it only when the last dev server closes.
