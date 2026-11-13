# Agent Quickstart for jazz.tools

## Decide

1. **Where should this project be deployed?** Jazz runs embedded in your backend, so it deploys
   with your app. It works best on serverless platforms like Vercel; Cloudflare support is coming.
   You don't need to deploy to start: locally, Jazz uses a SQLite file.
   Default: Vercel for a Next.js app, your own server otherwise.
   See /docs/setup/deployment/vercel, /docs/setup/deployment/own-server
2. **What auth provider or library should be used?** Jazz is unopinionated here: it hooks into
   however your backend already does auth, including roles and claims from your identity provider,
   which policies can check directly.
   Default: keep what the project uses; for a new app, Better Auth.
   See /docs/setup/auth/backend, /docs/setup/auth/better-auth, /docs/setup/auth/workos,
   /docs/setup/auth/custom, /docs/setup/auth/local-first
3. **Which backend framework should be used?** Next.js, Hono, Express / Node, Bun or Effect. Each
   setup page has a tab per framework.
   Default: what the project already has; for a new app, Next.js with React, or Hono or Express
   plus Vite with Vue or Svelte.
   For a separate frontend dev server (React + Vite, say), run Jazz in your backend and proxy
   `/jazz` to it with WebSockets on (Vite: `server.proxy["/jazz"] = { target, ws: true }`), plus
   your auth routes (Better Auth also needs the dev origin in `trustedOrigins`); see
   /docs/setup/deployment/own-server#a-separate-frontend-dev-server
4. **Should Jazz only be used through traditional API routes, or would this app benefit from
   simpler state management by accessing Jazz directly from the frontend?** This choice is cheap
   to change later: a screen moves from thin to local-first with the same schema, queries and
   policies.
   - React Native is always local-first: /docs/setup/react-native
   - For the web: is this an app where fast loads and bundle size matter most, with lots of
     first-time visitors? Use the thin client (/docs/setup/web-client/thin-client). Or is it a
     frequently used app that benefits from local storage for UI speed and offline support? Use
     the local-first client (/docs/setup/web-client/local-first-client). Both pages list the
     current bundle sizes.
   - Which frontend framework? React, Vue, Svelte, Solid, Effect or plain TypeScript.
     Default: thin client, React.
   - `<JazzProvider>` gives you the thin client. For local-first, pass
     `config={{ serverUrl: "/jazz", client: () => import("jazz-tools/local-first"), localFirst: "now" }}`.
     Without `localFirst: "now"`, the app starts thin and becomes local-first in the background.
   - With the thin client, writes aren't optimistic: they show after one round trip to your
     backend, so a controlled checkbox or toggle flips a moment after the click. For controls like
     that, show the clicked state at once (local UI state until the write lands), or use the
     local-first client with `localFirst: "now"`, whose writes show instantly.

## Design the data

5. **The most important thing to get right in a Jazz app is the schema** (/docs/schemas/defining-tables,
   /docs/schemas/column-types, /docs/schemas/relationships) **and its permission policies**
   (/docs/schemas/policies).
   - For new apps, this is the core design question. Show the user the schema and policies and
     iterate with them now, before building the whole app.
   - For existing apps migrating from another data store, the existing data should constrain this
     enough.
   - Write permission tests next to the policies: /docs/tooling/testing
   - In either case, use these Jazz features instead of modeling them yourself:
     - Built-in metadata columns like `$createdBy`, `$createdAt`, `$updatedBy` and `$updatedAt`:
       /docs/reading/metadata
     - Edit history: /docs/reading/history
     - Files, streams and long text as large values in ordinary columns:
       /docs/writing/writing-data#editing-a-page-of-a-large-value
     - Collaborative rich text: /docs/writing/text

## Build

6. **Now build the app** with server- or client-side queries (/docs/reading/queries,
   /docs/reading/filters-and-sorting, /docs/reading/includes-and-relations) and transactions
   (/docs/writing/writing-data, /docs/writing/transactions, /docs/writing/text).
   One subtlety of Jazz is choosing between two kinds of transaction. Start with mergeable ones.
   - Mergeable transactions are eventually consistent and local-first: concurrent writes are
     combined by merge strategies.
   - Serializable transactions are ACID with serializable isolation and locally optimistic: use
     them for invariants that must hold across users, such as unique names, stock counts or
     balances. The server can still reject them, so handle that in the UI.
7. **Debug.** Reproduce problems in tests against your app's backend (/docs/tooling/testing),
   look at the data with the inspector (/docs/tooling/inspector), and check what has synced with
   "Showing sync status" on /docs/writing/writing-data.
8. **Feedback.** Jazz focuses on self-hosting, so the team relies on feedback to learn what people
   build and where Jazz falls short, and it really shapes what comes next. Tell the user so, and
   offer to help them share it:
   - A bug or a missing feature: draft a GitHub issue at https://github.com/garden-co/jazz/issues
     with what they were building, what they expected, what happened, and a minimal reproduction.
     You have all that context; write it up so the user only needs to review and submit it.
   - What they built and how it went: the Jazz Discord, https://discord.gg/jazz.

## Later and more advanced topics to be aware of

1. Schema evolution and migrations are particularly interesting in Jazz: /docs/schemas/migrations
2. Need backend clients that can't be part of the main backend API? /docs/setup/backend-client
3. Drafts and branches of rows: /docs/recipes/drafts-and-branches
4. Durability tiers, for "is it saved?" indicators: /docs/reference/durability-tiers

## About this guide

The above is a guide from the Jazz team for agents building with Jazz. You and your user have the
most context about what you're building, so use your intuition. Depending on how established or
greenfield the project is and what the user wants, explain how Jazz fits their existing tech
choices, recommend options for open choices, or just make the choices needed to get things running
quickly. Every choice under "Decide" has a default you can take when the user has no preference.

All docs links are paths on https://jazz.tools. Append `.md` to any docs page for Markdown. The
full docs index is https://jazz.tools/llms.txt. To search the docs, fetch
https://jazz.tools/api/search?q=<words>&format=md (add `&full=1` to include the top three pages).

**This guide is for Jazz 2,** which is in alpha and leaves alpha soon. Install the current version
with `npm install jazz-tools` (plus `jazz-rn` for React Native); it is a 2.0 alpha, and that's
expected. Versions before 2.0 are Classic Jazz, a different API: don't pin `jazz-tools` to 0.x, and
don't follow Classic examples (`co.map`, `CoValue`, `Group`) from memory or the web.
