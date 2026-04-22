---
name: vite-plugin-parity
description: >
  Use this skill whenever you are reading, editing, or adding to any of the Jazz
  bundler plugins in packages/jazz-tools/src/dev/ (vite.ts, sveltekit.ts, nuxt.ts,
  next.ts, expo.ts). It checks that structural patterns are consistent across all
  five plugins and flags any gaps that look like unintentional omissions. Invoke it
  proactively when touching any of these files — even for small changes — because a
  one-line addition in one plugin often needs to be mirrored in the others.
---

# Vite Plugin Parity

The Jazz dev tooling is split across five bundler plugins. Four of them wrap a
shared `ManagedDevRuntime`, but the surface area of each plugin has grown
independently, so patterns can drift. This skill's job is to surface those drifts
so they can be fixed or consciously accepted.

## Plugin inventory

| File                                       | Export          | Build system                      |
| ------------------------------------------ | --------------- | --------------------------------- |
| `packages/jazz-tools/src/dev/vite.ts`      | `jazzPlugin`    | Vite (React / Vue / plain Svelte) |
| `packages/jazz-tools/src/dev/sveltekit.ts` | `jazzSvelteKit` | Vite + SvelteKit                  |
| `packages/jazz-tools/src/dev/nuxt.ts`      | `jazzNuxt`      | Vite + Nuxt                       |
| `packages/jazz-tools/src/dev/next.ts`      | `withJazz`      | Next.js (webpack / Turbopack)     |
| `packages/jazz-tools/src/dev/expo.ts`      | `withJazz`      | Expo / Metro                      |

## Step 1 — Read all five plugin files

Read each file in full before making any assessments. Do not rely on memory or
summaries; the files change and the parity check must reflect current code.

## Step 2 — Check the parity matrix

For each pattern below, determine whether each plugin implements it, then build a
matrix. Use ✅ (present), ❌ (missing — likely a gap), and `—` (intentionally
absent — see notes).

### Patterns for the Vite trio (vite.ts · sveltekit.ts · nuxt.ts)

These three share the same Vite plugin API and should behave consistently unless
a note below documents an intentional difference.

| Pattern                    | What to look for                                                                                                  |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| **WASM config hook**       | `config()` returns `worker.format: "es"` and `optimizeDeps.exclude` containing `"jazz-wasm"`                      |
| **Env file backfill**      | `loadEnvFileIntoProcessEnv(viteServer.config.root)` called inside `configureServer` before runtime initialisation |
| **Schema error surfacing** | `onSchemaError` callback passed to `runtime.initialize`, which calls `viteServer.ws.send({ type: "error", ... })` |
| **Runtime disposal**       | `viteServer.httpServer?.once("close", ...)` calls `runtime.dispose()`                                             |
| **Init error surfacing**   | `try/catch` around `runtime.initialize` that calls `viteServer.ws.send({ type: "error", ... })` on failure        |
| **backendSecret**          | `backendSecret` extracted from plugin options and passed to `runtime.initialize`                                  |
| **schemaDir default**      | Which directory is used when `options.schemaDir` is not set                                                       |

**Known intentional differences within the Vite trio:**

- `backendSecret` — `vite.ts` intentionally omits it. Plain Vite apps (React, Vue)
  have no server side, so there is no server that would consume the secret.
  `sveltekit.ts` and `nuxt.ts` expose it because both can have server routes that
  need it.

- `schemaDir` default — `sveltekit.ts` defaults to `src/lib/` (where SvelteKit
  keeps shared library code). `vite.ts` and `nuxt.ts` default to the project root.
  This is intentional and correct.

- `build.target` — intentionally absent from all plugins. Vite's own default
  (`baseline-widely-available`) already covers every feature Jazz needs,
  including BigInt. Injecting `"es2020"` would silently override a safer,
  broader default for no benefit. If a user deliberately targets an ancient
  baseline that lacks BigInt, that is their responsibility to manage.

### Patterns for Next.js and Expo

These two do not use the Vite plugin API, so they cannot implement Vite-specific
patterns (`config()` hook, `viteServer.ws.send`, `httpServer.once`). That is
intentional. However, the following structural patterns should still be present:

| Pattern                  | What to look for                                                      |
| ------------------------ | --------------------------------------------------------------------- |
| **Runtime disposal**     | Server cleanup on process exit or build-phase end                     |
| **Init error surfacing** | `try/catch` around runtime initialisation that surfaces errors        |
| **Env injection**        | Framework-appropriate env vars set (`NEXT_PUBLIC_*`, `EXPO_PUBLIC_*`) |

## Step 3 — Report

Output a filled-in parity matrix with a row per pattern and a column per plugin.
Then list:

**Likely gaps** — patterns marked ❌ that are not covered by a known intentional
difference. For each gap, write a concrete code suggestion showing what to add and
where. Keep suggestions minimal — match the style already present in the file.

**Intentional divergences** — only surface divergences that are _not_ already
documented in this skill. Known intentional differences (backendSecret, schemaDir
default, Vite-API patterns absent from Next.js/Expo) do not need to be listed —
the developer already knows about them. Only flag a divergence here if it looks
intentional but has no documented reason, so it can be recorded or clarified.

**Unknowns** — patterns where you cannot tell whether the absence is intentional.
Flag these for the developer to decide.

## Step 4 — If you are mid-edit

If this skill was invoked because a plugin is currently being changed, check
whether the change itself needs to be mirrored. After reporting the full matrix,
add a focused note: "Changes in this edit that need mirroring:" and list only the
files and specific additions that should be replicated.

## Reminders

- Read the files fresh every time — do not trust your context from earlier in the
  conversation for parity checks.
- A pattern being absent is not automatically a bug. Always cross-reference the
  intentional-differences list before flagging something as a gap.
- Suggest the smallest fix that closes the gap. Do not refactor surrounding code
  unless asked.
