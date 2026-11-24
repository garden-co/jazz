---
"jazz-tools": patch
---

A backend never follows a redirect from a remote core. A core reached through `remote()` or a `durableObjects()` URL that answers a request with a 3xx now fails with a `CoreRedirectError` naming the configured URL and where the core pointed, instead of a bare "answered with a redirect" (`durableObjects()` reported its `GET /app` redirect as "did not answer as a Jazz Durable Object core"). Configure the store with the core's final URL. Every request the backend's network transport sends uses `redirect: "manual"`, so no caller can forward the backend credential to another origin by following one.
