---
"jazz-tools": patch
---

Inspect and change an app's data through its own backend. A new `inspect` hook on `createJazz` decides who may do so. It grants unlimited access to every row, so gate it with your hosting provider's login or an SSO proxy in production. The backend serves the Inspector at `<basePath>/inspect/` and a JSON API at `<basePath>/inspect/api`. Agents use `jazz-tools inspect` or `jazz-tools inspect mcp` with a token a person approves in the browser. Every answer links to the Inspector, open on the same rows. In a browser, `window.__jazz.status()` and `window.__jazz.waitForSync()` report whether the page's writes reached the server.
