# Next.js Dev Plugin Design

## Overview

Add a Next.js dev plugin to `packages/jazz-tools` with the same core design choices as the existing Vite plugin:

- one small public factory
- zero-config local Jazz server startup in development
- opt-in attachment to an existing server
- schema publish on startup
- schema watch + re-publish during development
- environment-variable handoff to app code
- automatic cleanup when the dev process exits

The public API is a Next config wrapper:

```ts
import { withJazz } from "jazz-tools/dev/next";

export default withJazz({
  reactStrictMode: true,
});
```

The wrapper must stay lightweight and reuse the existing dev primitives:

- `startLocalJazzServer(...)`
- `pushSchemaCatalogue(...)`
- `watchSchema(...)`

It must not create a second, Next-specific server startup implementation.

## Goals

- Match the Vite plugin's behavioral model as closely as Next.js allows
- Keep the public API small and zero-config by default
- Reuse existing `jazz-tools/dev` primitives instead of forking behavior
- Inject a single canonical Next.js env contract for both client and server code
- Preserve existing user Next config instead of replacing it
- Work with the Next.js versions already used in this repo

## Hard Constraints

- The plugin is for `next dev`; `next build` and `next start` must not start or watch a Jazz dev server
- The public entrypoint is `withJazz(...)` from `jazz-tools/dev/next`
- The app-facing env names are exactly:
  - `NEXT_PUBLIC_JAZZ_APP_ID`
  - `NEXT_PUBLIC_JAZZ_SERVER_URL`
- No compatibility aliases should be injected for:
  - `JAZZ_APP_ID`
  - `JAZZ_SERVER_URL`
  - `NEXT_PUBLIC_APP_ID`
  - `NEXT_PUBLIC_SYNC_SERVER_URL`
- `adminSecret` must never be exposed through `NEXT_PUBLIC_*` env
- Existing user config must survive wrapping
- `serverExternalPackages` must include `jazz-napi` and `jazz-tools`

## Non-Goals

- Browser overlay integration that mimics Vite's WebSocket error overlay
- A separate runtime bootstrap file for Next apps
- Supporting legacy env variable names
- A deeper webpack- or Turbopack-specific plugin architecture
- Build-time schema validation changes outside this dev plugin

---

## Public API

### Entry point

Expose a new module:

```ts
import { withJazz } from "jazz-tools/dev/next";
```

Add it to package exports alongside the existing `./dev` and `./dev/vite` entries.

### Function shape

`withJazz` accepts:

1. a plain Next config object
2. a Next config function
3. optional Jazz plugin options

Proposed shape:

```ts
withJazz(nextConfig?, options?)
```

Where `options` follows the same user-facing contract as the Vite plugin:

```ts
type JazzPluginOptions = {
  server?: boolean | string | JazzServerOptions;
  adminSecret?: string;
  schemaDir?: string;
  appId?: string;
};
```

`JazzServerOptions` stays aligned with the Vite plugin and reuses the same type definition.

### Example usage

Zero-config local server:

```ts
import { withJazz } from "jazz-tools/dev/next";

export default withJazz({
  reactStrictMode: true,
});
```

Explicit server options:

```ts
import { withJazz } from "jazz-tools/dev/next";

export default withJazz(
  {
    reactStrictMode: true,
  },
  {
    server: {
      port: 4200,
      adminSecret: "dev-secret",
    },
    schemaDir: ".",
  },
);
```

Attach to an existing server:

```ts
import { withJazz } from "jazz-tools/dev/next";

export default withJazz(
  {
    reactStrictMode: true,
  },
  {
    server: "http://127.0.0.1:4200",
    adminSecret: "dev-secret",
    appId: "00000000-0000-0000-0000-000000000123",
  },
);
```

---

## Config Resolution Model

### Async wrapper

The wrapper must resolve through an async config path so it can perform dev startup during Next config resolution.

This is valid for the Next.js versions already used in the repo. Next resolves config exports asynchronously and supports async config functions / promises.

### Supported inputs

If the caller passes a config object, `withJazz` returns an async config function that resolves to that object plus Jazz changes.

If the caller passes a config function, `withJazz` awaits it first and then merges Jazz changes into the resolved config.

The wrapper must preserve the normal Next config function contract:

```ts
(phase, { defaultConfig }) => NextConfig | Promise<NextConfig>;
```

### Dev-only activation

Jazz startup only runs for `PHASE_DEVELOPMENT_SERVER`.

For every other phase, `withJazz` returns the resolved user config unchanged except for static config merging that is safe in all phases:

- preserve all existing fields
- preserve any existing `serverExternalPackages`
- union `serverExternalPackages` with:
  - `jazz-napi`
  - `jazz-tools`

No dev server startup, schema publish, schema watch, or Jazz env injection occurs outside the dev phase.

---

## Environment Contract

### Canonical env names

The plugin injects exactly two app-facing env variables:

```text
NEXT_PUBLIC_JAZZ_APP_ID
NEXT_PUBLIC_JAZZ_SERVER_URL
```

These names are the only supported env handoff contract for the Next plugin.

### Read path

When attaching to an existing server via env, the wrapper reads:

- `process.env.NEXT_PUBLIC_JAZZ_SERVER_URL`
- `process.env.NEXT_PUBLIC_JAZZ_APP_ID`

This works because Next loads `.env*` files before resolving `next.config`.

### Write path

During `next dev`, once the plugin has determined the active Jazz server and app ID, it must write those values to:

1. `process.env.NEXT_PUBLIC_JAZZ_APP_ID`
2. `process.env.NEXT_PUBLIC_JAZZ_SERVER_URL`
3. `nextConfig.env.NEXT_PUBLIC_JAZZ_APP_ID`
4. `nextConfig.env.NEXT_PUBLIC_JAZZ_SERVER_URL`

This gives both server-side and client-side app code one consistent contract.

### Secret handling

`adminSecret` is configuration for the plugin only. It is never copied to `nextConfig.env` and never exposed through `NEXT_PUBLIC_*`.

---

## Server Resolution Rules

The Next plugin follows the same server selection rules as the Vite plugin, translated to the new env names.

### `server: false`

Do nothing. No server startup, no schema push, no watcher, no env injection.

### Existing server from env

If `process.env.NEXT_PUBLIC_JAZZ_SERVER_URL` is already set:

- use that URL
- require `adminSecret` from plugin options
- derive `appId` from `process.env.NEXT_PUBLIC_JAZZ_APP_ID` or `options.appId`
- throw if `adminSecret` is missing
- throw if `appId` is missing

### Existing server from string

If `options.server` is a string:

- treat it as the Jazz server URL
- require `adminSecret`
- require `appId`
- throw if either is missing

### Local server

Otherwise, start a local server using the existing `startLocalJazzServer(...)` helper.

Defaults match the Vite plugin:

- generate `adminSecret` if one was not provided in server config
- derive `appId` from existing env, explicit config, or a fresh UUID
- default `port` to `0` for an ephemeral port

The plugin logs:

- server start URL
- data dir when present
- app ID
- schema publish success
- schema update success

The log prefix stays:

```text
[jazz]
```

---

## Lifecycle

### Singleton startup

Next may resolve config more than once during a single dev process. The plugin must therefore use a module-level singleton state so repeated config resolution does not:

- start duplicate Jazz servers
- create duplicate file watchers
- register duplicate shutdown hooks

The singleton state should track:

- whether initialization already ran
- the active server URL
- the active app ID
- the active admin secret
- the watcher handle, if any
- the local server handle, if the plugin started one
- whether shutdown hooks are already registered

### Startup sequence

For dev mode, initialization order is:

1. resolve the server target
2. determine `schemaDir`
3. determine `appId`
4. push schema with `pushSchemaCatalogue(...)`
5. start `watchSchema(...)`
6. inject `NEXT_PUBLIC_JAZZ_*` env values

This keeps the initial dev state aligned with the Vite plugin: startup does one explicit schema publish before enabling watcher-based incremental updates.

### Schema directory

`schemaDir` defaults to the Next project root, matching the Vite plugin's use of the app root as the default schema directory.

The caller can override it with `options.schemaDir`.

### Cleanup

Cleanup is process-based rather than bundler-hook-based.

Register one shared shutdown path for:

- `SIGINT`
- `SIGTERM`
- process exit / before-exit equivalent where safe

Cleanup order:

1. close the schema watcher
2. stop the local Jazz server if this plugin started it
3. clear singleton state

If the plugin is attached to an existing remote/local server that it did not start, cleanup must not attempt to stop that server.

---

## Config Merging Rules

### Preserve user config

`withJazz` must not replace user config. It merges narrowly on top of the resolved config.

### `env`

If `nextConfig.env` already exists:

- preserve all existing keys
- overwrite only:
  - `NEXT_PUBLIC_JAZZ_APP_ID`
  - `NEXT_PUBLIC_JAZZ_SERVER_URL`

### `serverExternalPackages`

`serverExternalPackages` must be treated as a set union:

- preserve existing user entries
- add `jazz-napi`
- add `jazz-tools`
- avoid duplicates

No other config fields should be changed unless required by the plugin's documented contract.

---

## Errors And Failure Behavior

### Existing server misconfiguration

These are hard configuration errors and must throw during config resolution:

- existing server URL provided without `adminSecret`
- existing server URL provided without `appId`

This matches the Vite plugin's fail-fast behavior for incomplete explicit configuration.

### Initial schema publish failure

If the initial `pushSchemaCatalogue(...)` fails:

- log a `[jazz] schema push failed: ...` message
- throw the error
- fail `next dev` startup

Unlike Vite, there is no Next-specific browser overlay channel to report through. Failing startup is the clearest equivalent.

### Watcher-time schema failure

If a later watcher-triggered push fails:

- log a `[jazz] schema push failed: ...` message
- keep the Next dev server running
- keep the watcher active so the next file change can retry

This matches the current watcher semantics: runtime watch errors are visible but not terminal.

### Watcher setup failure

If `watchSchema(...)` cannot start, the plugin should throw and fail dev startup. Running without a watcher would silently violate the plugin's contract.

---

## Testing Plan

Implementation must follow TDD. The first implementation step is adding failing tests for the new Next plugin.

### Required tests

1. `withJazz` preserves existing user config fields
2. `withJazz` unions `serverExternalPackages` with `jazz-napi` and `jazz-tools`
3. `withJazz` starts a local server in dev mode and injects:
   - `NEXT_PUBLIC_JAZZ_APP_ID`
   - `NEXT_PUBLIC_JAZZ_SERVER_URL`
4. `withJazz` pushes schema on startup
5. `withJazz` is a no-op for non-dev phases with respect to Jazz startup and Jazz env injection
6. `withJazz` throws when connecting to an existing server without `adminSecret`
7. `withJazz` throws when connecting to an existing server without `appId`
8. `withJazz` resolves config functions as well as plain config objects
9. `withJazz` does not create duplicate server/watcher instances when config resolution runs multiple times in one process

### Test style

Tests should mirror the Vite plugin tests:

- create a temporary schema directory with a real `schema.ts`
- start the plugin against a fake or minimal Next config resolution path
- verify server health and published schema through real HTTP calls
- verify env injection through the resolved config and `process.env`
- clean up all temp roots and env mutations after each test

### Example app follow-up

After implementation, update the existing Next example to consume:

- `process.env.NEXT_PUBLIC_JAZZ_APP_ID`
- `process.env.NEXT_PUBLIC_JAZZ_SERVER_URL`

and to adopt `withJazz(...)` in `next.config`.

That example update is part of implementation, not part of the plugin spec itself.
