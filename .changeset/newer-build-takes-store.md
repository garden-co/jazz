---
"jazz-tools": patch
---

After a deploy that changes the schema, the newer build now takes the browser's local-first store, and tabs of the previous build reload. Jazz orders builds by the schema history each one ships. A build whose history contains the other's came later.

- **When a newer build's tab opens**, the shared worker tells the older build's tabs. Each one stops taking writes (new ones throw until it reloads), makes the writes it already took durable, lets go of the store and reloads. A tab that hasn't let go after 5 seconds is closed. The newer tab then opens the store with its schema.
- **When a tab of an older build opens while a newer build has the store**, it reloads itself, at most once in 10 seconds, so a CDN still serving the old build can't make it loop.
- **To choose when an old tab reloads** (for example after a "new version available" notice), pass the new `createJazzClient` option `onNewerBuild: (reload) => …`.
- **Builds Jazz can't order** (histories neither contains, or one that ships none) still wait for each other, as with `held-by-another-tab`.
