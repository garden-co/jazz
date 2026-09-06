# next-hybrid

A Next.js local-first todo starter with optional Better Auth accounts.

## What this starter gives you

- An offline local-first account with an optional Better Auth identity.

## Getting started

Run `pnpm install` then `pnpm dev`, and set `BETTER_AUTH_SECRET` in `.env`.

The client creates a browser account manager and passes its selected opaque account handle to Jazz. Each SSR request remains independent; no credential or account-manager state is created in a server module.

New visitors use `getLoggedIn() ?? createLocalFirst()` and can work offline. Sign-up shuts down the old context after sync, then calls `linkJWT({ getToken })` outside it. Sign-in selects an existing external account with `loginJWT({ getToken })`. The resulting handle creates the next context. JWT refresh is supplied to the account manager as a credential callback rather than replacing a live context's identity.

The Better Auth memory adapter is only suitable for local development. Use a persistent adapter before deployment.

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
