# sveltekit-hybrid

A SvelteKit local-first todo starter with optional Better Auth accounts.

## What this starter gives you

- An offline local-first account with an optional Better Auth identity.

## Getting started

Run `pnpm install` then `pnpm dev`, with `BETTER_AUTH_SECRET` configured in `.env`.

The browser creates the account manager on mount, selects `getLoggedIn() ?? createLocalFirst()`, and supplies the opaque account handle to `JazzSvelteProvider`. Server rendering does not use a module-global credential.

After Better Auth sign-up, the active context shuts down with `shutdown({ waitForSync: true })`; `linkJWT({ getToken })` then runs outside the context and its selected handle opens the replacement. Sign-in calls `loginJWT({ getToken })`. Account credential callbacks handle refresh without rewriting a live context's JWT identity.

The bundled Better Auth memory adapter is for local development only; replace it with persistent storage before deployment.

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
