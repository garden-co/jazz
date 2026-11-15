# Jazz .65 upgrade test bed (built on published 2.0.0-alpha.64, 2026-11-15)

Each app is a frozen alpha.64 baseline for `npx jazz-tools upgrade` (files-as-source, .65).
Rerun everything: `./run-all.sh` (needs Node 24, pnpm 11, npm; Playwright for the Expo web check).
Per app: `cd <app> && pnpm install && npx jazz-tools validate --check && ../_common/run-smoke.sh . <port>`.
Shared pieces: `_common/server.ts` (Hono + createJazz + sqlite, bearer `user|claimsJSON` auth, device guests via `#app`/`#permissions` package imports), `_common/client.ts` (Node client helpers), `_common/run-smoke.sh`.

| App | Port | What `upgrade` must handle |
|---|---|---|
| t1-migrations | 8601 | 4 schema versions: initial, add optional column, **enum add-variant**, **rename via filled `m.renameFrom` TODO**; `exists`/`anyOf` policies with a helper closure; `fixture-data/` = a sqlite store written on .64 that the upgraded app must open |
| t2-computed | 8602 | `s.computed` (union return type, template string), **column `.transform({from,to})`** (timestamp <-> ISO string), `s.array`, `s.json`, `.optional()`; `tsOnly` appears in jazz.app.json; 2 versions |
| t3-permissions | 8603 | `exists` + `in`, `anyOf`/`allOf`, **claims** (`session.where({"claims.staff": true})`), `hopTo`, `allowedTo.read/update`, `whereOld`/`whereNew`, `ne`, `session.identity.subject`; backend-seeded rows (`/seed`) |
| t4 (Vercel) | - | Next.js + Better Auth on `vercel()`, project `jazz-eval-a64-next`; source tarball `app-a64-next.tgz` and `smoke/smoke.mjs` are in Improv thread #a64-gate-vercel-next |
| t5-expo | 8605 | Expo SDK 58 app (CJS) with shared schema in `jazz/` (`{"type":"module"}`), extensionless `./migrations` import (Metro), backend in `server/` via shim re-exports, `s.bytes()`; web export + local-first web check (`web-smoke.mjs`, run from a dir with playwright) |
| t6-import-style | 8606 | tables in separate modules (`src/db/tables/*`), `schema` imported as `schema` and as `jz`, **`defineSliceableApp` + `.slice()`** (slicing is removed in .65), permissions via `definePermissions` from `jazz-tools/permissions` in `src/db/access.ts`, root `permissions.ts` is a re-export, barrel `src/db/index.ts` re-exporting from `../../schema.js`; 2 versions |

| t7-enum-cases | 8607 | a **payload enum** (`s.enum({bug:{...},feature:{...}})`, removed in .65): `upgrade` must REFUSE with a clear error naming `tickets.kind` and write no partial files; baseline smoke covers only a row without a kind (jazz#5621) |

Baseline on 2.0.0-alpha.64 (npm): see BASELINE.md.
