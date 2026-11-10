---
"jazz-tools": patch
---

`remote()` takes one secret: `remote({ url, secret })`, the app's admin secret, replaces `remote({ url, adminSecret, backendSecret })`. Or pass `remote({ url, backendCredential })`, a function `createJazz` calls for each request and socket, such as one that returns your platform's OIDC token for a sync server started with `--trusted-workload-*`. The backend presents the credential in `X-Jazz-Backend-Credential` on every request and WebSocket upgrade to the core and states the access it needs in `X-Jazz-Backend-Access`. A browser's sync session reaches the core only after the backend admits its prelude, so a guest's session never gets more than its own token grants.

jazz-tools server: a server with an admin secret also accepts it as `X-Jazz-Backend-Credential`, with the access stated in `X-Jazz-Backend-Access` (`backend` or `admin`). Secrets a request or prelude offers itself don't count next to a credential. Requests without a credential work as before. Backend access through the credential works even on a server with no backend secret configured.

Upgrade the sync server first. A backend on this jazz-tools version presents the credential on every request, and a sync server older than this release refuses those requests. Jazz Cloud is upgraded before this jazz-tools release ships.
