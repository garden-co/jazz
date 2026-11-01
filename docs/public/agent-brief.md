Set up Jazz (https://jazz.tools) in this project. You do the work; ask me only the questions below.

1. Look at the project first. If it is empty, create the app with the framework's own
   scaffolder (create-next-app, create-vite, sv create, a plain Hono/Node app...) once I've
   answered the backend and client questions. Don't hand-write framework boilerplate.
2. Install jazz-tools. Add this to AGENTS.md (create it if missing):
   "Jazz: before writing Jazz code, fetch https://jazz.tools/llms.txt and the relevant page
   (append .md for Markdown). The installed jazz-tools types win over the docs."
   Follow that yourself from now on.
3. Ask me these in one message, each with your suggested answer and the docs link:
   - Where will Jazz run? Jazz Cloud or your own server. Default: own server locally
     for now. https://jazz.tools/docs/setup/deployment
   - Which backend? Hono, Next.js route handlers, Express, or none yet.
     https://jazz.tools/docs/setup/backend
   - Which client? Thin (default) or local-first; React, Svelte, Vue, vanilla, React Native.
     https://jazz.tools/docs/setup/web-client
   - Who are the users, and what is the data? Anonymous or an auth provider; the first
     tables and who may read or write each. https://jazz.tools/docs/setup/auth,
     https://jazz.tools/docs/schemas
4. Build in this order: schema.ts, then permissions.ts with permission tests, then mount
   Jazz in the backend with createJazz (jazz.server.ts, served under /jazz), then backend
   routes, then the client. There is no separate Jazz server to run and no deploy step.
5. You're done when you have shown me all of these working, and not before:
   - the dev setup starts with one command
   - a backend route writes a row that a live query in the UI shows without reload
   - a second user can't read rows the policies say are private, and the permission
     tests pass
6. Finish with: the commands to run, what you chose and why, and any docs that were wrong
   or missing (with URLs). Tell me about those; don't silently work around them.
