---
"create-jazz": patch
---

Wire the Jazz BetterAuth adapter into every starter that ships a BetterAuth backend — the six `*-betterauth` and `*-hybrid` starters across Next.js, SvelteKit, and React. Each starter now persists its auth tables in Jazz via `jazzAdapter` instead of the in-memory `memoryAdapter` stub, and publishes a `schema-better-auth/schema.ts` with the BetterAuth table definitions merged into the starter's main schema.

React starters pin the Jazz dev-server to port 4002 so the standalone Hono backend can connect without coordinating runtime values through `.env`, flip their dev script so Vite starts first (populating `VITE_JAZZ_APP_ID`) and tsx waits on it, and pre-generate a shared `BACKEND_SECRET` up front via `scripts/ensure-env.js`.
