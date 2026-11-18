---
"jazz-tools": patch
---

After a deploy that changes the schema, the newer build now takes the browser's local-first store, and tabs of the previous build reload. Jazz orders builds by the schema history each one ships. A build whose history contains the other's came later.

- **When a newer build's tab opens**, the shared worker tells the older build's tabs. Each one stops taking writes (new writes, queries and transaction commits throw until it reloads), makes the writes it already took durable, lets go of the store and reloads. A tab that hasn't let go after 5 seconds is closed as a crashed tab would be: what it had already handed to the shared worker is kept, and what it hadn't sent is lost. The newer tab then opens the store with its schema.
- **When a tab of an older build opens while a newer build has the store**, it reloads itself. If it loads the same build again (a CDN still serving it, or a rollback to the previous build, which Jazz orders as older), it doesn't reload again in that tab's session: it waits.
- **To choose when an old tab reloads** (for example after a "new version available" notice), pass the new `createJazzClient` option `onNewerBuild: (reload) => …`.
- **Builds Jazz can't order** (unrelated histories, or one that ships none) still wait for each other, as with `held-by-another-tab`.
