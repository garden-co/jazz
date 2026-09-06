# react-selfhosted-hybrid

A Vite + React local-first todo starter with optional Better Auth accounts.

## What this starter gives you

- An offline local-first account with an optional Better Auth identity.

## Getting started

Run `pnpm install` then `pnpm dev`. Set `BETTER_AUTH_SECRET` in `.env` before starting.

The browser prepares an account manager, then selects `accounts.getLoggedIn() ?? accounts.createLocalFirst()`. Jazz contexts receive only that opaque account handle. A local-first account works offline and syncs when the configured Jazz server is available.

After Better Auth creates an account, the app shuts down the current context with `await db.shutdown({ waitForSync: true })`, calls `accounts.linkJWT({ getToken })`, and mounts a new context with the returned selection. Existing accounts use `accounts.loginJWT({ getToken })`. Token refresh remains in the manager callback, so the app never swaps a JWT on a live context.

Recovery phrases and passkeys export or restore the selected local-first handle. Restore also shuts down its context before calling `restoreLocalFirst`.

The Hono server serves Better Auth at `/api/auth/*`. Its in-memory adapter is for development only; use a persistent adapter in production.

## Architecture

The browser owns the account manager and each context receives its selected handle.

## How it works

The manager performs enrollment and refresh; contexts remain identity-stable.

## Extending the schema

Edit `schema.ts` and `permissions.ts` for application data and access rules.

## Environment variables

Set `BETTER_AUTH_SECRET` and the Jazz application/server variables supplied by the plugin.

## Deploying to production

Use persistent Better Auth storage and deploy the configured Jazz server.

## Known limitations

The included Better Auth adapter is in-memory.

## Where to go next

Read the Jazz and Better Auth documentation before extending authentication.
