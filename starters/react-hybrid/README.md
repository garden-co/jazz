# react-selfhosted-hybrid

A Vite + React local-first todo starter with optional Better Auth accounts.

## What this starter gives you

- An offline local-first account with an optional Better Auth identity.

## Getting started

Run `pnpm install` then `pnpm dev`. Set `BETTER_AUTH_SECRET` in `.env` before starting.

The app configures one Jazz session with `initial: "local-first"`. It restores a usable saved account or creates a local-first account, which works offline and syncs when the server is available.

After Better Auth signup, the app calls `session.linkJWT({ getToken })`; sign-in calls `session.loginOrRegisterJWT({ getToken })`. The session detaches consumers, waits for sync, and replaces the client. A failed sync preserves the usable prior client and prevents enrollment. The credential callback supplies fresh tokens without changing a live client identity.

Recovery phrases and passkeys export or restore the selected local-first handle. Restore also shuts down its context before calling `restoreLocalFirst`.

The Hono server serves Better Auth at `/api/auth/*`. Its in-memory adapter is for development only; use a persistent adapter in production.

## Architecture

The Jazz session owns account selection and its active client.

## How it works

Session commands own enrollment and replacement; clients remain identity-stable.

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

This starter uses manual hybrid authentication. Do not attach `connectBetterAuth`
or `useBetterAuth`: signup must link the incoming identity before any automatic
account creation. Auth-required apps can use the corresponding Better Auth starter.
