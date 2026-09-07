# next-hybrid

A Next.js local-first todo starter with optional Better Auth accounts.

## What this starter gives you

- An offline local-first account with an optional Better Auth identity.

## Getting started

Run `pnpm install` then `pnpm dev`, and set `BETTER_AUTH_SECRET` in `.env`.

The app configures one Jazz session with `initial: "local-first"`. It restores a usable saved account or creates a local-first account, which works offline and syncs when the server is available.

After Better Auth signup, the app calls `session.linkJWT({ getToken })`; sign-in calls `session.loginJWT({ getToken })`. The session detaches consumers, waits for sync, and replaces the client. A failed sync preserves the usable prior client and prevents enrollment. The credential callback supplies fresh tokens without changing a live client identity.

The Better Auth memory adapter is only suitable for local development. Use a persistent adapter before deployment.

## Architecture

The Jazz session owns account selection and its active client.

## How it works

Session commands own enrollment and replacement; clients remain identity-stable.

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
