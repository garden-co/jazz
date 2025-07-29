# Example of using Jazz as database adapter for Better Auth

The `jazz-better-auth/database-adapter` is the database adapter to save Better Auth's users, accounts, and session on Jazz.

The only integration is inside the Better Auth definition in: [`src/lib/auth.ts`](./src/lib/auth.ts). It acts as [Worker](https://jazz.tools/docs/vanilla/server-side/setup) and persist everything on Jazz Sync Server.