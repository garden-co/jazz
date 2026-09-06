# ts-hybrid

A plain TypeScript + Vite local-first todo starter with optional Better Auth.

## What this starter gives you

- An offline local-first account with an optional Better Auth identity.

## Getting started

Run `pnpm install` then `pnpm dev`; set `BETTER_AUTH_SECRET` in `.env`.

Startup prepares an account manager and opens Jazz with `getLoggedIn() ?? createLocalFirst()`. The opaque account handle, rather than a secret or raw JWT, is the only credential passed to a Jazz context. This makes first use offline while permitting sync when the server is available.

For account linking, the app first awaits `db.shutdown({ waitForSync: true })`, then invokes `linkJWT({ getToken })` outside the retired context and opens the next one from the selected handle. Existing accounts use `loginJWT`. The credential callback owns token refresh and never changes identity on an existing Db.

The Hono Better Auth server uses an in-memory adapter for local development; replace it with persistent storage for production.

## Architecture

The browser owns the account manager and each context receives its selected handle.

## How it works

The manager performs enrollment and refresh; contexts remain identity-stable.

## Extending the schema

Edit `schema.ts` to add tables. The Jazz dev server watches the file and
republishes the schema on change — no restart needed.

```ts
const schema = {
  todos: s.table({ title: s.string(), done: s.boolean() }),
  projects: s.table({ name: s.string() }),
};
```

Row ownership is enforced by `permissions.ts` via the `$createdBy` predicate,
so you don't need an explicit `ownerId` column. Jazz records the creating
session on every row and the permission policy scopes reads/writes to it.

## Environment variables

Set `BETTER_AUTH_SECRET` and the Jazz application/server variables supplied by the plugin.

## Deploying to production

Use persistent Better Auth storage and deploy the configured Jazz server.

## Known limitations

The included Better Auth adapter is in-memory.

## Where to go next

Read the Jazz and Better Auth documentation before extending authentication.
